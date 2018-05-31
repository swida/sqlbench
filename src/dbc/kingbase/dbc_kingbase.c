/*
 * This file is released under the terms of the Artistic License.  Please see
 * the file LICENSE, included in this package, for details.
 *
 * Copyright (C) 2014 Wang Diancheng
 *
 * 7 Jan 2013
 */

#include <stdio.h>
#include <string.h>
#include <libkci.h>

#include "logging.h"
#include "transaction_data.h"
#include "dbc.h"
#include "common.h"
#include "db.h"

static char dbname[32] = "";
static char host[32] = "";
static char port[32] = "54321";
static char user[32] = "";

struct kingbase_context_t
{
	struct db_context_t base;
	KCIConnection *conn;
	int inTransaction;
};

#define LOAD_BUFFER_SIZE 16384
struct kingbase_loader_stream_t
{
	struct loader_stream_t base;
	char buffer[LOAD_BUFFER_SIZE];
	int cursor;
};

static int
kingbase_commit_transaction(struct db_context_t *_dbc)
{
	struct kingbase_context_t *dbc = (struct kingbase_context_t*) _dbc;
	KCIResult *res;
	int ret = OK;

	if(!dbc->inTransaction)
		return ret;

	res = KCIStatementExecute(dbc->conn, "COMMIT");
	if (!res || KCIResultGetStatusCode(res) != EXECUTE_COMMAND_OK) {
		LOG_ERROR_MESSAGE("%s", KCIConnectionGetLastError(dbc->conn));

		if (KCIConnectionGetStatus(dbc->conn) != CONNECTION_OK)
			dbc->base.need_reconnect = 1;

		ret = ERROR;
	}
	KCIResultDealloc(res);

	dbc->inTransaction = 0;

	return ret;
}

/* Open a connection to the database. */
static int
kingbase_connect_to_db(struct db_context_t *_dbc)
{
		struct kingbase_context_t *dbc = (struct kingbase_context_t*) _dbc;
        char buf[1024];
		char host_option[256] = "";

        if(*host != '\0')
			snprintf(host_option, sizeof(host_option), "host=%s", host);
		sprintf(buf, "%s port=%s dbname=%s user=%s", host_option, port, dbname, user);
        dbc->conn = KCIConnectionCreate(buf);
        if (KCIConnectionGetStatus(dbc->conn) != CONNECTION_OK) {
                LOG_ERROR_MESSAGE("Connection to database '%s' failed.",
                        dbname);
                LOG_ERROR_MESSAGE("%s", KCIConnectionGetLastError(dbc->conn));
                KCIConnectionDestory(dbc->conn);
		dbc->conn = NULL;
		dbc->base.need_reconnect = 1;

                return ERROR;
        }

		dbc->base.need_reconnect = 0;

        return OK;
}

/* Disconnect from the database and free the connection handle. */
static int
kingbase_disconnect_from_db(struct db_context_t *_dbc)
{
	struct kingbase_context_t *dbc = (struct kingbase_context_t*) _dbc;
	KCIConnectionDestory(dbc->conn);
	dbc->conn = NULL;
	return OK;
}

static struct db_context_t *
kingbase_db_init()
{
	struct db_context_t *context = malloc(sizeof(struct kingbase_context_t));
	memset(context, 0, sizeof(struct kingbase_context_t));
	return context;
}

static int
kingbase_rollback_transaction(struct db_context_t *_dbc)
{
	struct kingbase_context_t *dbc = (struct kingbase_context_t*) _dbc;
	KCIResult *res;
	int ret = STATUS_ROLLBACK;
	if(!dbc->inTransaction)
		return ret;
	
	res = KCIStatementExecute(dbc->conn, "ROLLBACK");
	if (!res || KCIResultGetStatusCode(res) != EXECUTE_COMMAND_OK) {
		LOG_ERROR_MESSAGE("%s", KCIConnectionGetLastError(dbc->conn));

		if (KCIConnectionGetStatus(dbc->conn) != CONNECTION_OK)
			dbc->base.need_reconnect = 1;

		ret = ERROR;
	}
	KCIResultDealloc(res);
	dbc->inTransaction = 0;

	return ret;
}

static int
kingbase_sql_execute(struct db_context_t *_dbc, char * query, struct sql_result_t * sql_result,
				  char * query_name)
{
	struct kingbase_context_t *dbc = (struct kingbase_context_t*) _dbc;
	KCIResult *res;

	if (!dbc->inTransaction)
	{
		/* Start a transaction block. */
		res = KCIStatementExecute(dbc->conn, "BEGIN");
		if (!res || KCIResultGetStatusCode(res) != EXECUTE_COMMAND_OK) {
			LOG_ERROR_MESSAGE("%s", KCIConnectionGetLastError(dbc->conn));
			KCIResultDealloc(res);

		if (KCIConnectionGetStatus(dbc->conn) != CONNECTION_OK)
			dbc->base.need_reconnect = 1;

			return ERROR;
		}

		KCIResultDealloc(res);
		dbc->inTransaction = 1;
	}
	res = KCIStatementExecute(dbc->conn, query);
	if (!res || (KCIResultGetStatusCode(res) != EXECUTE_COMMAND_OK &&
				 KCIResultGetStatusCode(res) != EXECUTE_TUPLES_OK)) {
		LOG_ERROR_MESSAGE("%s", KCIConnectionGetLastError(dbc->conn));
		KCIResultDealloc(res);

		if (KCIConnectionGetStatus(dbc->conn) != CONNECTION_OK)
			dbc->base.need_reconnect = 1;

		return ERROR;
	}

	if(sql_result == NULL)
		KCIResultDealloc(res);
	else
	{
		sql_result->result_set = res;
		sql_result->current_row_num = -1;
		sql_result->num_rows = KCIResultGetRowCount(res);
	}

	return OK;
}

static int
kingbase_sql_prepare(struct db_context_t *_dbc,  char *query, char *query_name)
{
	struct kingbase_context_t *dbc = (struct kingbase_context_t*) _dbc;

	KCIResult *res = KCIStatementPrepare(dbc->conn, query_name, query, 0, NULL);
	if (!res || KCIResultGetStatusCode(res) != EXECUTE_COMMAND_OK) {
		LOG_ERROR_MESSAGE("%s", KCIConnectionGetLastError(dbc->conn));
		KCIResultDealloc(res);

		if (KCIConnectionGetStatus(dbc->conn) != CONNECTION_OK)
			dbc->base.need_reconnect = 1;

		return ERROR;
	}

	KCIResultDealloc(res);
	return OK;
}

static int
kingbase_sql_execute_prepared(
	struct db_context_t *_dbc,
	char **params, int num_params, struct sql_result_t * sql_result,
	char * query_name)
{
	struct kingbase_context_t *dbc = (struct kingbase_context_t*) _dbc;
	KCIResult *res;

	if (!dbc->inTransaction)
	{
		/* Start a transaction block. */
		res = KCIStatementExecute(dbc->conn, "BEGIN");
		if (!res || KCIResultGetStatusCode(res) != EXECUTE_COMMAND_OK) {
			LOG_ERROR_MESSAGE("%s", KCIConnectionGetLastError(dbc->conn));
			KCIResultDealloc(res);

			if (KCIConnectionGetStatus(dbc->conn) != CONNECTION_OK)
				dbc->base.need_reconnect = 1;

			return ERROR;
		}

		KCIResultDealloc(res);
		dbc->inTransaction = 1;
	}
	res = KCIStatementExecutePrepared(dbc->conn, query_name, num_params, 1, (const char * const *)params, NULL, NULL, 0);
	if (!res || (KCIResultGetStatusCode(res) != EXECUTE_COMMAND_OK &&
				 KCIResultGetStatusCode(res) != EXECUTE_TUPLES_OK)) {
		LOG_ERROR_MESSAGE("%s", KCIConnectionGetLastError(dbc->conn));
		KCIResultDealloc(res);

		if (KCIConnectionGetStatus(dbc->conn) != CONNECTION_OK)
			dbc->base.need_reconnect = 1;

		return ERROR;
	}

	if(sql_result == NULL)
		KCIResultDealloc(res);
	else
	{
		sql_result->result_set = res;
		sql_result->current_row_num = -1;
		sql_result->num_rows = KCIResultGetRowCount(res);
	}

	return OK;
}

static int
kingbase_sql_fetchrow(struct db_context_t *_dbc, struct sql_result_t * sql_result)
{
	KCIResult *res = (KCIResult *)sql_result->result_set;
	sql_result->current_row_num++;
	if(sql_result->current_row_num >= KCIResultGetRowCount(res))
		return 0;
	return 1;
}

static int
kingbase_sql_close_cursor(struct db_context_t *_dbc, struct sql_result_t * sql_result)
{
	KCIResult *res = (KCIResult *)sql_result->result_set;
	KCIResultDealloc(res);
	return 1;
}

static char *
kingbase_sql_getvalue(struct db_context_t *_dbc, struct sql_result_t * sql_result, int field)
{
	KCIResult *res = (KCIResult *)sql_result->result_set;
	char *tmp = NULL;
	if (sql_result->current_row_num < 0 ||sql_result->current_row_num >= KCIResultGetRowCount(res) || field > KCIResultGetColumnCount(res))
	{
#ifdef DEBUG_QUERY
		LOG_ERROR_MESSAGE("kingbase_sql_getvalue: POSSIBLE NULL VALUE or ERROR\n\nRow: %d, Field: %d",
						  sql_result->current_row_num, field);
#endif
	return tmp;
	}

	if ((tmp = calloc(sizeof(char), KCIResultGetColumnValueLength(res, sql_result->current_row_num, field) + 1)))
		strcpy(tmp, KCIResultGetColumnValue(res, sql_result->current_row_num, field));
	else
		LOG_ERROR_MESSAGE("dbt2_sql_getvalue: CALLOC FAILED for value from field=%d\n", field);
	return tmp;
}
/* load opreations */
static struct loader_stream_t *
kingbase_open_loader_stream(struct db_context_t *_dbc, char *table_name, char delimiter, char *null_str)
{
  struct kingbase_context_t *dbc = (struct kingbase_context_t*) _dbc;
  char *buf;
  KCIResult *res;
  struct kingbase_loader_stream_t *stream = NULL;

  if ((buf = malloc(strlen(table_name) + strlen(null_str) + 64)) == NULL)
  {
	  LOG_ERROR_MESSAGE("out of memory\n");
	  return NULL;
  }

  sprintf(buf, "COPY %s FROM STDIN DELIMITER '%c' NULL '%s'", table_name, delimiter, null_str);

  res = KCIStatementExecute(dbc->conn, buf);
  free(buf);
  if (!res || KCIResultGetStatusCode(res) != EXECUTE_COPY_IN)
  {
	  LOG_ERROR_MESSAGE("%s", KCIConnectionGetLastError(dbc->conn));
	  KCIResultDealloc(res);

	  return NULL;
  }

  PQclear(res);

  if ((stream = malloc(sizeof(struct kingbase_loader_stream_t))) == NULL)
  {
	  LOG_ERROR_MESSAGE("out of memory\n");
	  KCICopySendEOF(dbc->conn, "client out of memory");
	  free(stream);
	  return NULL;
  }

  stream->base.dbc = (struct db_context_t *)dbc;
  stream->cursor = 0;

  return (struct loader_stream_t *)stream;
}

#define MAX_COPY_DATA_LEN 8192
static int
kingbase_write_to_stream(struct loader_stream_t *_stream, const char *fmt, va_list ap)
{
	struct kingbase_loader_stream_t *stream = (struct kingbase_loader_stream_t *) _stream;
	struct kingbase_context_t *dbc = (struct kingbase_context_t*) stream->base.dbc;

	stream->cursor += vsprintf(stream->buffer + stream->cursor, fmt, ap);

	if (stream->cursor > MAX_COPY_DATA_LEN)
	{
		if (KCICopySendData(dbc->conn, stream->buffer, stream->cursor) == -1)
		{
			LOG_ERROR_MESSAGE("fail to put copy data: %s", PQerrorMessage(dbc->conn));
			return -1;
		}
		stream->cursor = 0;
	}

	return 0;
}

static int kingbase_close_loader_stream(struct loader_stream_t *_stream)
{
	struct kingbase_loader_stream_t *stream = (struct kingbase_loader_stream_t *) _stream;
	struct kingbase_context_t *dbc = (struct kingbase_context_t*) stream->base.dbc;
	int ret = 0;

	if (stream->cursor > 0 && KCICopySendData(dbc->conn, stream->buffer, stream->cursor) == -1)
	{
		free(stream);
		LOG_ERROR_MESSAGE("fail to put copy data: %s", KCIConnectionGetLastError(dbc->conn));
		return -1;
	}

	if (KCICopySendEOF(dbc->conn, NULL) != -1)
	{
		KCIResult *res = KCIConnectionFetchResult(dbc->conn);
		KCIResultDealloc(res);
		ret = -1;
	}
	else
		LOG_ERROR_MESSAGE("fail to put copy data: %s", KCIConnectionGetLastError(dbc->conn));

	free(stream);

	return ret;
}

static struct option *
kingbase_dbc_get_options()
{
#define N_KINGBASE_OPT  4
	struct option *dbc_options = malloc(sizeof(struct option) * (N_KINGBASE_OPT + 1));

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

	dbc_options[N_KINGBASE_OPT].name = 0;
	dbc_options[N_KINGBASE_OPT].has_arg = 0;
	dbc_options[N_KINGBASE_OPT].flag = 0;
	dbc_options[N_KINGBASE_OPT].val = 0;
	return dbc_options;
}

static int
kingbase_dbc_set_option(const char *optname, const char *optvalue)
{
	if(strncmp(optname, "dbname", 32) == 0 && optvalue != NULL)
		strncpy(dbname, optvalue, 32);
	else if(strncmp(optname, "host", 32) == 0 && optvalue != NULL)
		strncpy(host, optvalue, 32);
	else if(strncmp(optname, "port", 32) == 0 && optvalue != NULL)
	{
		/* check port */
		int nport = atoi(optvalue);
		if(nport < 0 || nport > 65535)
		{
			/* XXX: a better way to raise a message. */
			printf("invalid port number: %s\n", optvalue);
			return ERROR;
		}
		strncpy(port, optvalue, 32);
	}
	else if(strncmp(optname, "user", 32) == 0 && optvalue != NULL)
		strncpy(user, optvalue, 32);
	return OK;
}

struct dbc_sql_operation_t kingbase_sql_operation =
{
	kingbase_db_init,
	kingbase_connect_to_db,
	kingbase_disconnect_from_db,
	kingbase_commit_transaction,
	kingbase_rollback_transaction,
	kingbase_sql_execute,
	kingbase_sql_prepare,
	kingbase_sql_execute_prepared,
	kingbase_sql_fetchrow,
	kingbase_sql_close_cursor,
	kingbase_sql_getvalue
};

static struct dbc_loader_operation_t kingbase_loader_operation =
{
	kingbase_open_loader_stream,
	kingbase_write_to_stream,
	kingbase_close_loader_stream
};

extern struct dbc_storeproc_operation_t kingbase_storeproc_operation;

int
kingbase_dbc_init()
{
	struct dbc_info_t *kingbase_info = make_dbc_info(
		"kingbase",
		"for kingbase: --dbname=<dbname> --host=<host> --port=<port> --user=<user>");
	kingbase_info->is_forupdate_supported = 0;

	kingbase_info->dbc_sql_operation = &kingbase_sql_operation;
	kingbase_info->dbc_storeproc_operation = NULL;
	kingbase_info->dbc_loader_operation = &kingbase_loader_operation;
	kingbase_info->dbc_get_options = kingbase_dbc_get_options;
	kingbase_info->dbc_set_option = kingbase_dbc_set_option;
	dbc_manager_add(kingbase_info);
	return OK;
}
