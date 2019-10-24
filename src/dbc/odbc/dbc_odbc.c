/*
 * This file is released under the terms of the Artistic License.  Please see
 * the file LICENSE, included in this package, for details.
 *
 * Copyright (C) 2002 Mark Wong & Jenny Zhang &
 *		    Open Source Development Labs, Inc.
 *
 * 11 June 2002
 */

#include <stdio.h>
#include <string.h>
#include <stdarg.h>
#include <sql.h>
#include <sqlext.h>
#include <pthread.h>

#include "logging.h"
#include "transaction_data.h"
#include "dbc.h"
#include "common.h"
#include "db.h"

static SQLHENV henv = SQL_NULL_HENV;
static pthread_mutex_t db_source_mutex = PTHREAD_MUTEX_INITIALIZER;

struct odbc_context_t
{
	struct db_context_t base;
	SQLHDBC hdbc;
	SQLHSTMT hstmt;
};

struct odbc_result_set
{
	SQLSMALLINT num_fields;
	SQLULEN *lengths;
};

static SQLCHAR servername[32];
static SQLCHAR username[32];
static SQLCHAR authentication[32];

static int
check_odbc_rc(SQLSMALLINT handle_type, SQLHANDLE handle, SQLRETURN rc)
{

	if (rc == SQL_SUCCESS) {
		return OK;
	} else if (rc == SQL_SUCCESS_WITH_INFO) {
		LOG_ERROR_MESSAGE("SQL_SUCCESS_WITH_INFO");
	} else if (rc == SQL_NEED_DATA) {
		LOG_ERROR_MESSAGE("SQL_NEED_DATA");
	} else if (rc == SQL_STILL_EXECUTING) {
		LOG_ERROR_MESSAGE("SQL_STILL_EXECUTING");
	} else if (rc == SQL_ERROR) {
		return ERROR;
	} else if (rc == SQL_NO_DATA) {
		LOG_ERROR_MESSAGE("SQL_NO_DATA");
	} else if (rc == SQL_INVALID_HANDLE) {
		LOG_ERROR_MESSAGE("SQL_INVALID_HANDLE");
	}

	return OK;
}

/* Print out all errors messages generated to the error log file. */
static int
log_odbc_error(char *filename, int line, SQLSMALLINT handle_type,
		SQLHANDLE handle)
{
	SQLCHAR sqlstate[5];
	SQLCHAR message[256];
	SQLSMALLINT i;
	char msg[1024];

	i = 1;
	while (SQLGetDiagRec(handle_type, handle, i, sqlstate,
		NULL, message, sizeof(message), NULL) == SQL_SUCCESS) {
		sprintf(msg, "[%d] sqlstate %s : %s", i, sqlstate, message);
		log_error_message(filename, line, msg);
		++i;
	}
	return OK;
}

#define LOG_ODBC_ERROR(type, handle) log_odbc_error(__FILE__, __LINE__, type, handle)

static int
odbc_commit_transaction(struct db_context_t *_dbc)
{
	int i;
	struct odbc_context_t *dbc = (struct odbc_context_t*) _dbc;

	i = SQLEndTran(SQL_HANDLE_DBC, dbc->hdbc, SQL_COMMIT);
	if (i != SQL_SUCCESS && i != SQL_SUCCESS_WITH_INFO) {
		LOG_ODBC_ERROR(SQL_HANDLE_STMT, dbc->hstmt);
		return ERROR;
	}

	return OK;
}

/* Open an ODBC connection to the database. */
static int odbc_connect_to_db(struct db_context_t *dbc)
{
	SQLRETURN rc;
	struct odbc_context_t *odbcc = (struct odbc_context_t*) dbc;

	/* Allocate connection handles. */
	pthread_mutex_lock(&db_source_mutex);
	rc = SQLAllocHandle(SQL_HANDLE_DBC, henv, &odbcc->hdbc);
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO) {
		LOG_ODBC_ERROR(SQL_HANDLE_DBC, odbcc->hdbc);
		SQLFreeHandle(SQL_HANDLE_DBC, odbcc->hdbc);
		return ERROR;
	}

	/* Open connection to the database. */
	rc = SQLConnect(odbcc->hdbc, servername, SQL_NTS,
			username, SQL_NTS, authentication, SQL_NTS);
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO) {
		LOG_ODBC_ERROR(SQL_HANDLE_DBC, odbcc->hdbc);
		SQLFreeHandle(SQL_HANDLE_DBC, odbcc->hdbc);
		return ERROR;
	}

	rc = SQLSetConnectAttr(odbcc->hdbc, SQL_ATTR_AUTOCOMMIT,
			SQL_AUTOCOMMIT_OFF, 0);
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO) {
		LOG_ODBC_ERROR(SQL_HANDLE_STMT, odbcc->hstmt);
		return ERROR;
	}

	rc = SQLSetConnectAttr(odbcc->hdbc, SQL_ATTR_TXN_ISOLATION,
			(SQLPOINTER *) SQL_TXN_REPEATABLE_READ, 0);
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO) {
		LOG_ODBC_ERROR(SQL_HANDLE_STMT, odbcc->hstmt);
		return ERROR;
	}

	/* allocate statement handle */
	rc = SQLAllocHandle(SQL_HANDLE_STMT, odbcc->hdbc, &odbcc->hstmt);
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO) {
		LOG_ODBC_ERROR(SQL_HANDLE_STMT, odbcc->hstmt);
		return ERROR;
	}

	pthread_mutex_unlock(&db_source_mutex);

	return OK;
}

/*
 * Disconnect from the database and free the connection handle.
 * Note that we create the environment handle in odbc_connect() but
 * we don't touch it here.
 */
static int
odbc_disconnect_from_db(struct db_context_t *dbc)
{
	SQLRETURN rc;
	struct odbc_context_t *odbcc = (struct odbc_context_t*) dbc;

	pthread_mutex_lock(&db_source_mutex);
	rc = SQLDisconnect(odbcc->hdbc);
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO) {
		LOG_ODBC_ERROR(SQL_HANDLE_DBC, odbcc->hdbc);
		return ERROR;
	}
	rc = SQLFreeHandle(SQL_HANDLE_DBC, odbcc->hdbc);
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO) {
		LOG_ODBC_ERROR(SQL_HANDLE_DBC, odbcc->hdbc);
		return ERROR;
	}
	rc = SQLFreeHandle(SQL_HANDLE_STMT, odbcc->hstmt);
	if (rc != SQL_SUCCESS) {
		LOG_ODBC_ERROR(SQL_HANDLE_STMT, odbcc->hstmt);
		return ERROR;
	}
	pthread_mutex_unlock(&db_source_mutex);
	return OK;
}

/* Initialize ODBC environment handle and the database connect string. */
static struct db_context_t *
odbc_db_init()
{
	struct db_context_t *context;
	SQLRETURN rc;

	/* Initialized the environment handle. */
	rc = SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &henv);
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO) {
		LOG_ERROR_MESSAGE("alloc env handle failed");
		return NULL;
	}
	rc = SQLSetEnvAttr(henv, SQL_ATTR_ODBC_VERSION, (void *) SQL_OV_ODBC3, 0);
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO) {
		SQLFreeHandle(SQL_HANDLE_ENV, henv);
		return NULL;
	}

	context = malloc(sizeof(struct odbc_context_t));
	memset(context, 0, sizeof(struct odbc_context_t));
	return context;
}

static int
odbc_rollback_transaction(struct db_context_t *_dbc)
{
	int i;
	struct odbc_context_t *dbc = (struct odbc_context_t*) _dbc;

	i = SQLEndTran(SQL_HANDLE_DBC, dbc->hdbc, SQL_ROLLBACK);
	if (i != SQL_SUCCESS && i != SQL_SUCCESS_WITH_INFO) {
		LOG_ODBC_ERROR(SQL_HANDLE_STMT, dbc->hstmt);
		return ERROR;
	}

	return STATUS_ROLLBACK;
}

static int
odbc_sql_execute(struct db_context_t *_dbc, char *query,
		struct sql_result_t *sql_result, char *query_name)
{
	int i;
	SQLCHAR colname[32];
	SQLSMALLINT coltype;
	SQLSMALLINT colnamelen;
	SQLSMALLINT scale;
	SQLLEN num_rows;
	SQLRETURN rc;
	struct odbc_context_t *dbc = (struct odbc_context_t*) _dbc;
	struct odbc_result_set *odbc_rs;

	if (sql_result)
	{
		odbc_rs = malloc(sizeof(struct odbc_result_set));
		odbc_rs->num_fields= 0;
		odbc_rs->lengths = NULL;
		sql_result->result_set = odbc_rs;
		sql_result->num_rows= 0;
	}

	rc = SQLExecDirect(dbc->hstmt, query, SQL_NTS);
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO) {
		LOG_ODBC_ERROR(SQL_HANDLE_STMT, dbc->hstmt);
		return 0;
	}

	if (!sql_result)
		return OK;

	rc = SQLNumResultCols(dbc->hstmt, &odbc_rs->num_fields);
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO) {
		LOG_ODBC_ERROR(SQL_HANDLE_STMT, dbc->hstmt);
		return 0;
	}

	rc = SQLRowCount(dbc->hstmt, &num_rows);
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)  {
		LOG_ODBC_ERROR(SQL_HANDLE_STMT, dbc->hstmt);
		return 0;
	}
	sql_result->num_rows = (int) num_rows;

	if (odbc_rs->num_fields) {
		odbc_rs->lengths= malloc(sizeof(SQLULEN) * odbc_rs->num_fields);

		for (i=0; i < odbc_rs->num_fields; i++) {
			SQLDescribeCol(dbc->hstmt,
					(SQLSMALLINT)(i + 1),
					colname,
					sizeof(colname),
					&colnamelen,
					&coltype,
					&odbc_rs->lengths[i],
					&scale,
					NULL
			);
		}
		sql_result->current_row_num = 1;
	}

	return OK;
}

static int
odbc_sql_close_cursor(struct db_context_t *_dbc,
		struct sql_result_t * sql_result)
{
	SQLRETURN   rc;
	struct odbc_result_set *odbc_rs = sql_result->result_set;
	struct odbc_context_t *dbc = (struct odbc_context_t*) _dbc;

	if (odbc_rs->lengths) {
		free(odbc_rs->lengths);
		odbc_rs->lengths = NULL;
	}
	free(odbc_rs);

	rc = SQLCloseCursor(dbc->hstmt);
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO) {
		LOG_ODBC_ERROR(SQL_HANDLE_STMT, dbc->hstmt);
		return 0;
	}
	return OK;
}


static int
odbc_sql_fetchrow(struct db_context_t *_dbc,
		struct sql_result_t * sql_result)
{
	SQLRETURN  rc;
	struct odbc_context_t *dbc = (struct odbc_context_t*) _dbc;

	rc = SQLFetch(dbc->hstmt);

	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO) {
		LOG_ODBC_ERROR(SQL_HANDLE_STMT, dbc->hstmt);
		/* result set - NULL */
		sql_result->current_row_num= 0;
		return 0;
	}

  return 1;
}

static char *
odbc_sql_getvalue(struct db_context_t *_dbc,
				  struct sql_result_t *sql_result, int field)
{
	SQLRETURN rc;
	char *tmp = NULL;
	SQLLEN cb_var = 0;
	struct odbc_context_t *dbc = (struct odbc_context_t*) _dbc;
	struct odbc_result_set *odbc_rs = sql_result->result_set;

	if (sql_result->current_row_num && field < odbc_rs->num_fields) {
		if ((tmp = calloc(sizeof(char), odbc_rs->lengths[field] + 1))) {
			rc = SQLGetData(dbc->hstmt, field + 1, SQL_C_CHAR, tmp,
					odbc_rs->lengths[field] + 1, &cb_var);
			if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO) {
				LOG_ODBC_ERROR(SQL_HANDLE_STMT, dbc->hstmt);
			}
		} else {
			LOG_ERROR_MESSAGE("dbt2_sql_getvalue: CALLOC FAILED for value from field=%d\n", field);
		}
	}

	return tmp;
}

static char *mysql_get_last_error(struct db_context_t *_dbc)
{
	struct odbc_context_t *dbc = (struct odbc_context_t*) _dbc;
	return mysql_error(dbc->mysql);
}

static struct option *
odbc_dbc_get_options()
{
#define N_PGSQL_OPT  3
	struct option *dbc_options = malloc(sizeof(struct option) * (N_PGSQL_OPT + 1));

	dbc_options[0].name = "servername";
	dbc_options[0].has_arg = required_argument;
	dbc_options[0].flag = NULL;
	dbc_options[0].val = 0;

	dbc_options[1].name = "username";
	dbc_options[1].has_arg = required_argument;
	dbc_options[1].flag = NULL;
	dbc_options[1].val = 0;

	dbc_options[2].name = "authentication";
	dbc_options[2].has_arg = required_argument;
	dbc_options[2].flag = NULL;
	dbc_options[2].val = 0;

	dbc_options[N_PGSQL_OPT].name = 0;
	dbc_options[N_PGSQL_OPT].has_arg = 0;
	dbc_options[N_PGSQL_OPT].flag = 0;
	dbc_options[N_PGSQL_OPT].val = 0;
	return dbc_options;
}

static int
odbc_dbc_set_option(const char *optname, const char *optvalue)
{
	if(strcmp(optname, "servername") == 0 && optvalue != NULL)
		strncpy(servername, optvalue, sizeof(servername));
	else if(strcmp(optname, "username") == 0 && optvalue != NULL)
		strncpy(username, optvalue, sizeof(username));
	else if(strcmp(optname, "authentication") == 0 && optvalue != NULL)
		strncpy(authentication, optvalue, sizeof(authentication));

	return OK;
}

static struct dbc_sql_operation_t odbc_sql_operation =
{
	odbc_db_init,
	odbc_connect_to_db,
	odbc_disconnect_from_db,
	odbc_commit_transaction,
	odbc_rollback_transaction,
	odbc_sql_execute,
	NULL,
	NULL,
	odbc_sql_fetchrow,
	odbc_sql_close_cursor,
	odbc_sql_getvalue
};

int
odbc_dbc_init()
{
	struct dbc_info_t *odbc_info = make_dbc_info(
		"odbc",
		"for odbc: --servername=<servername> --username=<username> --authentication=<auth>");
	odbc_info->is_forupdate_supported = 0;
	odbc_info->dbc_sql_operation = &odbc_sql_operation;
	odbc_info->dbc_storeproc_operation = NULL;
	odbc_info->dbc_loader_operation = NULL,
	odbc_info->dbc_get_options = odbc_dbc_get_options;
	odbc_info->dbc_set_option = odbc_dbc_set_option;
	dbc_manager_add(odbc_info);
	return OK;
}
