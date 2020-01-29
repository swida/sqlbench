/*
 * This file is released under the terms of the Artistic License.  Please see
 * the file LICENSE, included in this package, for details.
 *
 * Copyright (C) 2002 Mark Wong & Open Source Development Labs, Inc.
 *
 * 13 May 2003
 */

#include <stdio.h>
#include <string.h>
#include <stdarg.h>
#include <libpq-fe.h>

#include "logging.h"
#include "transaction_data.h"
#include "dbc.h"
#include "common.h"
#include "db.h"

struct pgsql_context_t
{
	db_context_t base;
	PGconn *conn;
	int inTransaction;
};

#define LOAD_BUFFER_SIZE 16384
struct pgsql_loader_stream_t
{
	struct loader_stream_t base;
	char buffer[LOAD_BUFFER_SIZE];
	int cursor;
};

static char dbname[16][128] = {""};
static char pgport[32] = "";
static char pguser[128] = "";
static char pghost[128] = "";
static char pgpass[128] = "";
static int dbname_count = 0;
static int last_dbname_connected = 0;

static int
pgsql_commit_transaction(db_context_t *_dbc)
{
	struct pgsql_context_t *dbc = (struct pgsql_context_t*) _dbc;
	PGresult *res;
	int ret = OK;

	if(!dbc->inTransaction)
		return ret;

	res = PQexec(dbc->conn, "COMMIT");
	if (!res || PQresultStatus(res) != PGRES_COMMAND_OK) {
		LOG_ERROR_MESSAGE("%s", PQerrorMessage(dbc->conn));

		if (PQstatus(dbc->conn) != CONNECTION_OK)
			dbc->base.need_reconnect = 1;

		ret = ERROR;
	}
	PQclear(res);

	dbc->inTransaction = 0;

	return ret;
}

/* Open a connection to the database. */
static int
pgsql_connect_to_db(db_context_t *_dbc)
{
	struct pgsql_context_t *dbc = (struct pgsql_context_t*) _dbc;
	char buf[1024];
	int index = 0;

	/* speical case: dbname is conninfo string */
	if (dbname[0] != NULL && strchr(dbname[0], '=') != NULL)
	{
		/* just conninfo string case support connect to multi-instance
		 * database */
		dbc->conn = PQconnectdb(dbname[last_dbname_connected++]);
		if (last_dbname_connected >= dbname_count)
			last_dbname_connected = 0;
		if (PQstatus(dbc->conn) != CONNECTION_OK) {
			LOG_ERROR_MESSAGE("using conninfo string '%s' connect to database failed.", dbname);
			LOG_ERROR_MESSAGE("%s", PQerrorMessage(dbc->conn));
			PQfinish(dbc->conn);
			dbc->conn = NULL;
			return ERROR;
		}
		return OK;
	}

	if (*dbname[0] != '\0')
		index += snprintf(buf + index, sizeof(buf) - index, "dbname=%s ", dbname[0]);
	if (*pghost != '\0')
		index += snprintf(buf + index, sizeof(buf) - index, "host=%s ", pghost);
	if (*pgport != '\0')
		index += snprintf(buf + index, sizeof(buf) - index, "port=%s ", pgport);
	if (*pguser != '\0')
		index += snprintf(buf + index, sizeof(buf) - index, "user=%s ", pguser);
	if (*pgpass != '\0')
		index += snprintf(buf + index, sizeof(buf) - index, "password=%s ", pgpass);
	dbc->conn = PQconnectdb(buf);
	if (PQstatus(dbc->conn) != CONNECTION_OK) {
		LOG_ERROR_MESSAGE("Connection to database '%s' failed.",
						  dbname);
		LOG_ERROR_MESSAGE("%s", PQerrorMessage(dbc->conn));
		PQfinish(dbc->conn);
		dbc->conn = NULL;
		dbc->base.need_reconnect = 1;

		return ERROR;
	}

	dbc->base.need_reconnect = 0;

	return OK;
}

/* Disconnect from the database and free the connection handle. */
static int
pgsql_disconnect_from_db(db_context_t *_dbc)
{
	struct pgsql_context_t *dbc = (struct pgsql_context_t*) _dbc;
	PQfinish(dbc->conn);
	dbc->conn = NULL;
	return OK;
}

static db_context_t *
pgsql_db_init()
{
	db_context_t *context = malloc(sizeof(struct pgsql_context_t));
	memset(context, 0, sizeof(struct pgsql_context_t));
	return context;
}

static int
pgsql_rollback_transaction(db_context_t *_dbc)
{
	struct pgsql_context_t *dbc = (struct pgsql_context_t*) _dbc;
	PGresult *res;

	int ret = STATUS_ROLLBACK;

	if (!dbc->inTransaction)
		return ret;

	res = PQexec(dbc->conn, "ROLLBACK");
	if (!res || PQresultStatus(res) != PGRES_COMMAND_OK) {
		LOG_ERROR_MESSAGE("%s", PQerrorMessage(dbc->conn));

		if (PQstatus(dbc->conn) != CONNECTION_OK)
			dbc->base.need_reconnect = 1;

		ret = ERROR;
	}

	PQclear(res);
	dbc->inTransaction = 0;

	return ret;
}

static int
pgsql_sql_execute(db_context_t *_dbc, char * query, struct sql_result_t *sql_result,
				  char *query_name)
{
	struct pgsql_context_t *dbc = (struct pgsql_context_t*) _dbc;
	PGresult *res;

	if (!dbc->inTransaction)
	{
		/* Start a transaction block. */
		res = PQexec(dbc->conn, "BEGIN");
		if (!res || PQresultStatus(res) != PGRES_COMMAND_OK) {
			LOG_ERROR_MESSAGE("%s", PQerrorMessage(dbc->conn));
			PQclear(res);

			if (PQstatus(dbc->conn) != CONNECTION_OK)
				dbc->base.need_reconnect = 1;

			return ERROR;
		}

		PQclear(res);
		dbc->inTransaction = 1;
	}
	res = PQexec(dbc->conn, query);
	if (!res || (PQresultStatus(res) != PGRES_COMMAND_OK &&
				 PQresultStatus(res) != PGRES_TUPLES_OK)) {
		LOG_ERROR_MESSAGE("%s", PQerrorMessage(dbc->conn));
		PQclear(res);

		if (PQstatus(dbc->conn) != CONNECTION_OK)
			dbc->base.need_reconnect = 1;

		return ERROR;
	}

	if(sql_result == NULL)
		PQclear(res);
	else
	{
		sql_result->result_set = res;
		sql_result->current_row_num = -1;
		sql_result->num_rows = PQntuples(res);
	}

	return OK;
}

static void *
pgsql_sql_prepare(db_context_t *_dbc,  char *query, void *query_name)
{
	struct pgsql_context_t *dbc = (struct pgsql_context_t*) _dbc;

	PGresult *res = PQprepare(dbc->conn, (char *)query_name, query, 0, NULL);
	if (!res || PQresultStatus(res) != PGRES_COMMAND_OK) {
		LOG_ERROR_MESSAGE("%s", PQerrorMessage(dbc->conn));
		PQclear(res);

		if (PQstatus(dbc->conn) != CONNECTION_OK)
			dbc->base.need_reconnect = 1;

		return NULL;
	}

	PQclear(res);
	return query_name;
}

static int
pgsql_sql_execute_prepared(
	db_context_t *_dbc,
	char **params, int num_params, struct sql_result_t * sql_result,
	void *query_name)
{
	struct pgsql_context_t *dbc = (struct pgsql_context_t*) _dbc;
	PGresult *res;

	if (!dbc->inTransaction)
	{
		/* Start a transaction block. */
		res = PQexec(dbc->conn, "BEGIN");
		if (!res || PQresultStatus(res) != PGRES_COMMAND_OK) {
			LOG_ERROR_MESSAGE("%s", PQerrorMessage(dbc->conn));
			PQclear(res);

			if (PQstatus(dbc->conn) != CONNECTION_OK)
				dbc->base.need_reconnect = 1;

			return ERROR;
		}

		PQclear(res);
		dbc->inTransaction = 1;
	}
	res = PQexecPrepared(
		dbc->conn, (char *)query_name,
		num_params, (const char * const *)params, NULL, NULL, 0);
	if (!res || (PQresultStatus(res) != PGRES_COMMAND_OK &&
				 PQresultStatus(res) != PGRES_TUPLES_OK)) {
		LOG_ERROR_MESSAGE("%s", PQerrorMessage(dbc->conn));
		PQclear(res);

		if (PQstatus(dbc->conn) != CONNECTION_OK)
			dbc->base.need_reconnect = 1;

		return ERROR;
	}

	if(sql_result == NULL)
		PQclear(res);
	else
	{
		sql_result->result_set = res;
		sql_result->current_row_num = -1;
		sql_result->num_rows = PQntuples(res);
	}

	return OK;
}

static int
pgsql_sql_fetchrow(db_context_t *_dbc, struct sql_result_t * sql_result)
{
	PGresult *res = (PGresult *)sql_result->result_set;
	sql_result->current_row_num++;
	if(sql_result->current_row_num >= PQntuples(res))
		return 0;
	return 1;
}

static int
pgsql_sql_close_cursor(db_context_t *_dbc, struct sql_result_t * sql_result)
{
	PGresult *res = (PGresult *)sql_result->result_set;
	PQclear(res);
	return 1;
}

static char *
pgsql_sql_getvalue(db_context_t *_dbc, struct sql_result_t * sql_result, int field)
{
	PGresult *res = (PGresult *)sql_result->result_set;
	char *tmp = NULL;
	if (sql_result->current_row_num < 0 ||sql_result->current_row_num >= PQntuples(res) || field > PQnfields(res))
	{
#ifdef DEBUG_QUERY
		LOG_ERROR_MESSAGE("pgsql_sql_getvalue: POSSIBLE NULL VALUE or ERROR\n\nRow: %d, Field: %d",
						  sql_result->current_row_num, field);
#endif
	return tmp;
	}

	if ((tmp = calloc(sizeof(char), PQgetlength(res, sql_result->current_row_num, field) + 1)))
		strcpy(tmp, PQgetvalue(res, sql_result->current_row_num, field));
	else
		LOG_ERROR_MESSAGE("dbt2_sql_getvalue: CALLOC FAILED for value from field=%d\n", field);
	return tmp;
}

/* load opreations */
static struct loader_stream_t *
pgsql_open_loader_stream(db_context_t *_dbc, const char *table_name, char delimiter, char *null_str)
{
  struct pgsql_context_t *dbc = (struct pgsql_context_t*) _dbc;
  char *buf;
  PGresult *res;
  struct pgsql_loader_stream_t *stream = NULL;

  if ((buf = malloc(strlen(table_name) + strlen(null_str) + 64)) == NULL)
  {
	  LOG_ERROR_MESSAGE("out of memory\n");
	  return NULL;
  }

  sprintf(buf, "COPY %s FROM STDIN DELIMITER '%c' NULL '%s'", table_name, delimiter, null_str);

  res = PQexec(dbc->conn, buf);
  free(buf);
  if (!res || PQresultStatus(res) != PGRES_COPY_IN)
  {
	  LOG_ERROR_MESSAGE("%s", PQerrorMessage(dbc->conn));
	  PQclear(res);

	  return NULL;
  }

  PQclear(res);

  if ((stream = malloc(sizeof(struct pgsql_loader_stream_t))) == NULL)
  {
	  LOG_ERROR_MESSAGE("out of memory\n");
	  PQputCopyEnd(dbc->conn, "client out of memory");
	  free(stream);
	  return NULL;
  }

  stream->base.dbc = (db_context_t *)dbc;
  stream->cursor = 0;

  return (struct loader_stream_t *)stream;
}

#define MAX_COPY_DATA_LEN 8192
static int
pgsql_write_to_stream(struct loader_stream_t *_stream, const char *fmt, va_list ap)
{
	struct pgsql_loader_stream_t *stream = (struct pgsql_loader_stream_t *) _stream;
	struct pgsql_context_t *dbc = (struct pgsql_context_t*) stream->base.dbc;

	stream->cursor += vsprintf(stream->buffer + stream->cursor, fmt, ap);

	if (stream->cursor > MAX_COPY_DATA_LEN)
	{
		if (PQputCopyData(dbc->conn, stream->buffer, stream->cursor) == -1)
		{
			LOG_ERROR_MESSAGE("fail to put copy data: %s", PQerrorMessage(dbc->conn));
			return -1;
		}
		stream->cursor = 0;
	}

	return 0;
}

static int pgsql_close_loader_stream(struct loader_stream_t *_stream)
{
	struct pgsql_loader_stream_t *stream = (struct pgsql_loader_stream_t *) _stream;
	struct pgsql_context_t *dbc = (struct pgsql_context_t*) stream->base.dbc;
	int ret = 0;

	if (stream->cursor > 0 && PQputCopyData(dbc->conn, stream->buffer, stream->cursor) == -1)
	{
		free(stream);
		LOG_ERROR_MESSAGE("fail to put copy data: %s", PQerrorMessage(dbc->conn));
		return -1;
	}

	if (PQputCopyEnd(dbc->conn, NULL) != -1)
	{
		PGresult *res = PQgetResult(dbc->conn);
		PQclear(res);
		ret = -1;
	}
	else
		LOG_ERROR_MESSAGE("fail to put copy data: %s", PQerrorMessage(dbc->conn));

	free(stream);

	return ret;
}

static struct option *
pgsql_dbc_get_options()
{
#define N_PGSQL_OPT  5
	struct option *dbc_options = malloc(sizeof(struct option) * (N_PGSQL_OPT + 1));

	dbc_options[0].name = "dbname";
	dbc_options[0].has_arg = required_argument;
	dbc_options[0].flag = NULL;
	dbc_options[0].val = 0;

	dbc_options[1].name = "host";
	dbc_options[1].has_arg = required_argument;
	dbc_options[1].flag = NULL;
	dbc_options[1].val = 0;

	dbc_options[2].name = "port";
	dbc_options[2].has_arg = required_argument;
	dbc_options[2].flag = NULL;
	dbc_options[2].val = 0;

	dbc_options[3].name = "user";
	dbc_options[3].has_arg = required_argument;
	dbc_options[3].flag = NULL;
	dbc_options[3].val = 0;

	dbc_options[4].name = "password";
	dbc_options[4].has_arg = required_argument;
	dbc_options[4].flag = NULL;
	dbc_options[4].val = 0;

	dbc_options[N_PGSQL_OPT].name = 0;
	dbc_options[N_PGSQL_OPT].has_arg = 0;
	dbc_options[N_PGSQL_OPT].flag = 0;
	dbc_options[N_PGSQL_OPT].val = 0;
	return dbc_options;
}

static int
pgsql_dbc_set_option(const char *optname, const char *optvalue)
{
	if(strcmp(optname, "dbname") == 0 && optvalue != NULL &&
	   dbname_count < sizeof(dbname) / sizeof(dbname[0]))
		strncpy(dbname[dbname_count++], optvalue, sizeof(dbname));
	else if(strcmp(optname, "host") == 0 && optvalue != NULL)
		strncpy(pghost, optvalue, sizeof(pghost));
	else if(strcmp(optname, "port") == 0 && optvalue != NULL)
	{
		/* check port */
		int port = atoi(optvalue);
		if(port < 0 || port > 65535)
		{
			/* XXX: a better way to raise a message. */
			printf("invalid port number: %s\n", optvalue);
			return ERROR;
		}
		strncpy(pgport, optvalue, sizeof(pgport));
	}
	else if(strcmp(optname, "user") == 0 && optvalue != NULL)
		strncpy(pguser, optvalue, sizeof(pguser));
	else if(strcmp(optname, "password") == 0 && optvalue != NULL)
		strncpy(pgpass, optvalue, sizeof(pgpass));
	return OK;
}

static struct dbc_sql_operation_t pgsql_sql_operation =
{
	pgsql_db_init,
	pgsql_connect_to_db,
	pgsql_disconnect_from_db,
	pgsql_commit_transaction,
	pgsql_rollback_transaction,
	pgsql_sql_execute,
	pgsql_sql_prepare,
	pgsql_sql_execute_prepared,
	pgsql_sql_fetchrow,
	pgsql_sql_close_cursor,
	pgsql_sql_getvalue
};

static struct dbc_loader_operation_t pgsql_loader_operation =
{
	pgsql_open_loader_stream,
	pgsql_write_to_stream,
	pgsql_close_loader_stream
};

extern struct dbc_storeproc_operation_t pgsql_storeproc_operation;

int
pgsql_dbc_init()
{
	struct dbc_info_t *pgsql_info = make_dbc_info(
		"postgresql",
		"for postgresql: --dbname=<dbname> --host=<host> --port=<port> --user=<user> --password=<pass>");
	pgsql_info->is_forupdate_supported = 1;

	pgsql_info->dbc_sql_operation = &pgsql_sql_operation;
	pgsql_info->dbc_storeproc_operation = &pgsql_storeproc_operation;
	pgsql_info->dbc_loader_operation = &pgsql_loader_operation;
	pgsql_info->dbc_get_options = pgsql_dbc_get_options;
	pgsql_info->dbc_set_option = pgsql_dbc_set_option;
	dbc_manager_add(pgsql_info);
	return OK;
}
