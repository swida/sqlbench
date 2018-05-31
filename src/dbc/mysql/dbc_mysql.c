#include <string.h>
#include <mysql.h>
#include <mysql_com.h>
#include "logging.h"
#include "db.h"
#include "dbc.h"

struct mysql_context_t
{
	struct db_context_t base;
	MYSQL *mysql;
	int inTransaction;
};

#define LOAD_BUFFER_SIZE 16384
struct mysql_loader_stream_t
{
	struct loader_stream_t base;
	char buffer[LOAD_BUFFER_SIZE];
	int cursor;
};

static char my_dbname[32];
static char my_host[32];
static char my_port[32];
static char my_user[32];
static char my_pass[32];
static char my_sock[256];

static int
mysql_commit_transaction(struct db_context_t *_dbc)
{
	struct mysql_context_t *dbc = (struct mysql_context_t*) _dbc;
	if(!dbc->inTransaction)
		return OK;

	if (mysql_real_query(dbc->mysql, "COMMIT", 6)) 
	{
        LOG_ERROR_MESSAGE("COMMIT failed. mysql reports: %d %s", 
						  mysql_errno(dbc->mysql), mysql_error(dbc->mysql));
        return ERROR;
	}

	dbc->inTransaction = 1;

	return OK;
}

static int
mysql_rollback_transaction(struct db_context_t *_dbc)
{
	struct mysql_context_t *dbc = (struct mysql_context_t*) _dbc;

	if (!dbc->inTransaction)
		return STATUS_ROLLBACK;

	if (mysql_real_query(dbc->mysql, "ROLLBACK", 8)) 
	{
        LOG_ERROR_MESSAGE("ROLLBACK failed. mysql reports: %d %s", 
						  mysql_errno(dbc->mysql), mysql_error(dbc->mysql));
        return ERROR;
	}

	dbc->inTransaction = 0;

	return STATUS_ROLLBACK;
}

static struct db_context_t *
mysql_db_init()
{
	struct db_context_t *context = malloc(sizeof(struct mysql_context_t));
	memset(context, 0, sizeof(struct mysql_context_t));
	return context;
}

/* Open a connection to the database. */
static int
mysql_connect_to_db(struct db_context_t *_dbc)
{
	struct mysql_context_t *dbc = (struct mysql_context_t*) _dbc;
    dbc->mysql=mysql_init(NULL);

    //FIXME: change atoi() to strtol() and check for errors
    if (!mysql_real_connect(dbc->mysql, my_host, my_user, my_pass, my_dbname, atoi(my_port), my_sock, 0))
    {
		if (mysql_errno(dbc->mysql))
		{
			LOG_ERROR_MESSAGE("Connection to database '%s' failed.", my_dbname);
			LOG_ERROR_MESSAGE("mysql reports: %d %s", 
							mysql_errno(dbc->mysql), mysql_error(dbc->mysql));
		}
		dbc->base.need_reconnect = 1;
		return ERROR;
    }

    /* Disable AUTOCOMMIT mode for connection */
    if (mysql_real_query(dbc->mysql, "SET AUTOCOMMIT=0", 16))
    {
      LOG_ERROR_MESSAGE("mysql reports: %d %s", mysql_errno(dbc->mysql) ,
                         mysql_error(dbc->mysql));
      return ERROR;
    }

	dbc->base.need_reconnect = 0;

    return OK;
}

/* Disconnect from the database and free the connection handle. */
static int
mysql_disconnect_from_db(struct db_context_t *_dbc)
{
	struct mysql_context_t *dbc = (struct mysql_context_t*) _dbc;
	mysql_close(dbc->mysql);
	dbc->mysql = NULL;

	return OK;
}

static int
mysql_sql_execute(struct db_context_t *_dbc, char *query,
					  struct sql_result_t *sql_result, 
					  char *query_name)
{

	struct mysql_context_t *dbc = (struct mysql_context_t*) _dbc;

	if (!dbc->inTransaction)
	{
		/* Start a transaction block. */
		if (mysql_real_query(dbc->mysql, "COMMIT", 6)) 
		{
			LOG_ERROR_MESSAGE("COMMIT failed. mysql reports: %d %s", 
							  mysql_errno(dbc->mysql), mysql_error(dbc->mysql));
			return ERROR;
		}

		dbc->inTransaction = 1;
	}

	if (mysql_query(dbc->mysql, query))
	{
		LOG_ERROR_MESSAGE("%s: %s\nmysql reports: %d %s",query_name, query,
						  mysql_errno(dbc->mysql), mysql_error(dbc->mysql));
		return ERROR;
	}
	else if (sql_result != NULL)
	{
		sql_result->result_set = mysql_store_result(dbc->mysql);

		if (sql_result->result_set)
			sql_result->num_rows= mysql_num_rows(sql_result->result_set);
		else  
		{
			if (mysql_field_count(dbc->mysql) == 0)
				sql_result->num_rows = mysql_affected_rows(dbc->mysql);
			else 
			{
				LOG_ERROR_MESSAGE("%s: %s\nmysql reports: %d %s",query_name, query,
								  mysql_errno(dbc->mysql), mysql_error(dbc->mysql));
				return ERROR;
			}
		}
	}

	return OK;
}

static int
mysql_sql_fetchrow(struct db_context_t *_dbc, struct sql_result_t * sql_result)
{
	(void) _dbc;
	
	if ((sql_result->current_row = mysql_fetch_row(sql_result->result_set)) == NULL)
		return 0;
#if 0
	if (sql_result->current_row)
	{
		sql_result->lengths= mysql_fetch_lengths(sql_result->result_set);
		return 1;
	}
#endif
	return 1;
}

static int
mysql_sql_close_cursor(struct db_context_t *_dbc, struct sql_result_t * sql_result)
{
	if (!sql_result->result_set)
		return 1;

	mysql_free_result(sql_result->result_set);
	return 1;
}

static char *
mysql_sql_getvalue(struct db_context_t *_dbc, struct sql_result_t * sql_result, int field)
{
	long *lengths = mysql_fetch_lengths(sql_result->result_set);
	long num_fields = mysql_num_fields(sql_result->result_set);
	char *tmp;

	if (!sql_result->current_row || field >= num_fields)
	{
#ifdef DEBUG_QUERY
		LOG_ERROR_MESSAGE("mysql_sql_getvalue: POSSIBLE NULL VALUE or ERROR\n\nRow: %d, Field: %d",
						  sql_result->current_row_num, field);
#endif
		return NULL;
	}
	if ((tmp = calloc(sizeof(char), lengths[field]+1)))
	{
        memcpy(tmp, ((MYSQL_ROW)(sql_result->current_row))[field], lengths[field]);
	}
	else
	{
        LOG_ERROR_MESSAGE("dbt2_sql_getvalue: CALLOC FAILED for value from field=%d\n", field);
	}

	return tmp;
}

extern ulong cli_safe_read(MYSQL *mysql);
extern ulong STDCALL net_field_length(unsigned char **pos);

/* load opreations */
static struct loader_stream_t *
mysql_open_loader_stream(struct db_context_t *_dbc, char *table_name, char delimiter, char *null_str)
{
	struct mysql_context_t *dbc = (struct mysql_context_t*) _dbc;
	char *buf;
	struct mysql_loader_stream_t *stream = NULL;
	NET *net = &dbc->mysql->net;
	unsigned char *pos;

	/* ENABLE AUTOCOMMIT mode for connection when loading data */
    if (mysql_real_query(dbc->mysql, "SET AUTOCOMMIT=1", 16))
    {
      LOG_ERROR_MESSAGE("count not set autocommit, mysql reports: (%d) %s", mysql_errno(dbc->mysql) ,
                         mysql_error(dbc->mysql));
      return NULL;
    }

	if ((buf = malloc(strlen(table_name) * 2 + strlen(null_str) + 128)) == NULL)
	{
		LOG_ERROR_MESSAGE("out of memory\n");
		return NULL;
	}

	sprintf(buf, "load data local infile '%s' into table %s fields terminated by '%c'", table_name, table_name, delimiter);

	if (mysql_send_query(dbc->mysql, buf, strlen(buf)))
  {
	  free(buf);
	  LOG_ERROR_MESSAGE("could not execute load data local infile statement: %s",
						 mysql_error(dbc->mysql));
	  return NULL;
  }
  free(buf);

  if (cli_safe_read(dbc->mysql) == packet_error)
  {
	  LOG_ERROR_MESSAGE("fail to read data from mysql server: %s",
						mysql_error(dbc->mysql));
	  return NULL;
  }

  pos=(unsigned char*) dbc->mysql->net.read_pos;
  if (net_field_length(&pos) != NULL_LENGTH)
  {
	  LOG_ERROR_MESSAGE("unexpected packet from mysql server");
	  return NULL;
  }

  if ((stream = malloc(sizeof(struct mysql_loader_stream_t))) == NULL)
  {
	  LOG_ERROR_MESSAGE("out of memory\n");
	  (void) my_net_write(net,(const unsigned char*) "",0); /* Server needs one packet */
	  net_flush(net);

	  return NULL;
  }

  stream->base.dbc = (struct db_context_t *)dbc;
  stream->cursor = 0;

  return (struct loader_stream_t *)stream;

}

#define MAX_PACK_DATA_LEN 8192
static int
mysql_write_to_stream(struct loader_stream_t *_stream, const char *fmt, va_list ap)
{
	struct mysql_loader_stream_t *stream = (struct mysql_loader_stream_t *) _stream;
	struct mysql_context_t *dbc = (struct mysql_context_t*) stream->base.dbc;
	NET *net = &dbc->mysql->net;

	stream->cursor += vsprintf(stream->buffer + stream->cursor, fmt, ap);

	if (stream->cursor > MAX_PACK_DATA_LEN)
	{
		 if (my_net_write(net, stream->buffer, stream->cursor))
		 {
			 LOG_ERROR_MESSAGE("fail to send data to mysql server, server lost.");
			 return -1;
		 }

		 stream->cursor = 0;
	}

	return 0;
}


static int mysql_close_loader_stream(struct loader_stream_t *_stream)
{
	struct mysql_loader_stream_t *stream = (struct mysql_loader_stream_t *) _stream;
	struct mysql_context_t *dbc = (struct mysql_context_t*) stream->base.dbc;
	NET *net = &dbc->mysql->net;
	int ret;

	if (stream->cursor > 0 && my_net_write(net, stream->buffer, stream->cursor))
	{
		free(stream);
		LOG_ERROR_MESSAGE("fail to send data to mysql server, server lost.");
		return -1;
	}

	/* Send empty packet to mark end of file */
	if (my_net_write(net, "", 0) || net_flush(net))
	{
		ret = -1;
		LOG_ERROR_MESSAGE("fail to send data to mysql server, server lost.");
	}
	mysql_read_query_result(dbc->mysql);

	free(stream);

	return ret;
}

static struct option *
mysql_dbc_get_options()
{
#define N_MYSQL_OPT  6
	struct option *dbc_options = malloc(sizeof(struct option) * (N_MYSQL_OPT + 1));

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

	dbc_options[5].name = "socket";
	dbc_options[5].has_arg = required_argument;
	dbc_options[5].flag = NULL;
	dbc_options[5].val = 0;

	dbc_options[N_MYSQL_OPT].name = 0;
	dbc_options[N_MYSQL_OPT].has_arg = 0;
	dbc_options[N_MYSQL_OPT].flag = 0;
	dbc_options[N_MYSQL_OPT].val = 0;
	return dbc_options;
}

static int
mysql_dbc_set_option(const char *optname, const char *optvalue)
{
	if(strcmp(optname, "dbname") == 0 && optvalue != NULL)
	   strncpy(my_dbname, optvalue, sizeof(my_dbname));
	else if(strcmp(optname, "host") == 0 && optvalue != NULL)
		strncpy(my_host, optvalue, sizeof(my_host));
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
		strncpy(my_port, optvalue, sizeof(my_port));
	}
	else if (strcmp(optname, "user") == 0 && optvalue != NULL)
		strncpy(my_user, optvalue, sizeof(my_user));
	else if (strcmp(optname, "password") == 0 && optvalue != NULL)
		strncpy(my_pass, optvalue, sizeof(my_pass));
	else if (strcmp(optname, "socket") == 0 && optvalue != NULL)
		strncpy(my_sock, optvalue, sizeof(my_sock));
	return OK;
}

static struct dbc_sql_operation_t mysql_sql_operation =
{
	mysql_db_init,
	mysql_connect_to_db,
	mysql_disconnect_from_db,
	mysql_commit_transaction,
	mysql_rollback_transaction,
	mysql_sql_execute,
	NULL,
	NULL,
	mysql_sql_fetchrow,
	mysql_sql_close_cursor,
	mysql_sql_getvalue
};

static struct dbc_loader_operation_t mysql_loader_operation =
{
	mysql_open_loader_stream,
	mysql_write_to_stream,
	mysql_close_loader_stream
};

int
mysql_dbc_init()
{
	struct dbc_info_t *mysql_info = make_dbc_info(
		"mysql",
		"for mysql: --dbname=<dbname> --host=<host> --port=<port> --user=<user> --password=<pass> --socket=<socket>");
	mysql_info->is_forupdate_supported = 0;

	mysql_info->dbc_sql_operation = &mysql_sql_operation;
	mysql_info->dbc_storeproc_operation = NULL;
	mysql_info->dbc_loader_operation = &mysql_loader_operation;
	mysql_info->dbc_get_options = mysql_dbc_get_options;
	mysql_info->dbc_set_option = mysql_dbc_set_option;
	dbc_manager_add(mysql_info);
	return OK;
}
