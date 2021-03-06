#include "config.h"
#include "dbc.h"
#include "common.h"
#include <string.h>
#include <stdarg.h>
#ifdef ENABLE_POSTGRESQL
extern int pgsql_dbc_init();
#endif
#ifdef ENABLE_MYSQL
extern int mysql_dbc_init();
#endif
#ifdef ENABLE_ODBC
extern int odbc_dbc_init();
#endif
struct dbc_construct_t dbc_constructs[] =
{
#ifdef ENABLE_POSTGRESQL
	{pgsql_dbc_init},
#endif
#ifdef ENABLE_MYSQL
	{mysql_dbc_init},
#endif
#ifdef ENABLE_ODBC
	{odbc_dbc_init},
#endif
	{NULL},
};

struct dbc_desc_t
{
	struct dbc_info_t *dbc_info;
	struct dbc_desc_t *dbc_next;
};

/* configurations */
int _is_forupdate_supported;

static struct dbc_desc_t dbc_desc = {NULL, NULL};
static char output_dbc_names[64] = "";
static char output_dbc_usages[512] = "";

/* user final selected dbc */
static struct dbc_info_t *_dbc_info = NULL;

struct dbc_info_t *make_dbc_info(const char *dbcname, const char *dbcusage)
{
	struct dbc_info_t *info = malloc(sizeof(struct dbc_info_t));
	info->dbc_name = (char *)dbcname;
	info->dbc_usage = (char *)dbcusage;
	/* not support storeproc testing by default */
	info->dbc_storeproc_operation = NULL;

	/* default configurations */
	info->is_forupdate_supported = 1;

	return info;
}

void
init_dbc_manager()
{
	int i;
	for(i = 0; dbc_constructs[i].dbc_init != NULL; i++)
		(*(dbc_constructs[i].dbc_init))();

	struct dbc_desc_t *curr_desc = dbc_desc.dbc_next;
	while(curr_desc != NULL)
	{
		int len = strlen(output_dbc_names);
		output_dbc_names[len++] = ' ';
		strcpy(output_dbc_names + len, curr_desc->dbc_info->dbc_name);
		len = strlen(output_dbc_usages);
		output_dbc_usages[len++] = '\t';
		strcpy(output_dbc_usages + len, curr_desc->dbc_info->dbc_usage);
		strcat(output_dbc_usages, "\n");
		curr_desc = curr_desc->dbc_next;
	}
}

void
dbc_manager_add(struct dbc_info_t *dbc_info)
{
	struct dbc_desc_t *curr_desc = &dbc_desc;
	struct dbc_desc_t *desc_add;

	while(curr_desc->dbc_next != NULL)
		curr_desc = curr_desc->dbc_next;
	desc_add = malloc(sizeof(struct dbc_desc_t));
	desc_add->dbc_info = dbc_info;
	desc_add->dbc_next = NULL;
	curr_desc->dbc_next = desc_add;
}

const char *
dbc_manager_get_dbcnames()
{
	return output_dbc_names;
}

const char *
dbc_manager_get_dbcusages()
{
	return output_dbc_usages;
}

int dbc_manager_set(char *dname)
{
	struct dbc_desc_t *curr_desc = dbc_desc.dbc_next;
	while(curr_desc != NULL)
	{
		if(strncmp(dname, curr_desc->dbc_info->dbc_name, 16) == 0)
		{
			_dbc_info = curr_desc->dbc_info;
			break;
		}
		curr_desc = curr_desc->dbc_next;
	}
	if(_dbc_info == NULL)
		return ERROR;

	/* inital configurations */
	_is_forupdate_supported = _dbc_info->is_forupdate_supported;

	return OK;
}

struct option * dbc_manager_get_dbcoptions()
{
	assert(_dbc_info);
	return (*(_dbc_info->dbc_get_options))();
}

int dbc_manager_set_dbcoption(const char *optname, const char *optvalue)
{
	assert(_dbc_info);
	return (*(_dbc_info->dbc_set_option))(optname, optvalue);
}

char *dbc_manager_get_name()
{
	return _dbc_info->dbc_name;
}

int dbc_manager_is_storeproc_supported()
{
	assert(_dbc_info);
	return _dbc_info->dbc_storeproc_operation != NULL;
}

int dbc_manager_is_extended_supported()
{
	assert(_dbc_info);
	assert(_dbc_info->dbc_sql_operation);
	struct dbc_sql_operation_t *sop = _dbc_info->dbc_sql_operation;

	return sop->sql_prepare != NULL;
}

/* interfaces */
db_context_t *
dbc_db_init(void)
{
	assert(_dbc_info);
	struct dbc_sql_operation_t *sop = _dbc_info->dbc_sql_operation;
	return (*(sop->db_init))();
}
int
dbc_connect_to_db(db_context_t *dbc)
{
	assert(_dbc_info);
	struct dbc_sql_operation_t *sop = _dbc_info->dbc_sql_operation;
	return (*(sop->connect_to_db))(dbc);
}

int
dbc_disconnect_from_db(db_context_t *dbc)
{
	assert(_dbc_info);
	struct dbc_sql_operation_t *sop = _dbc_info->dbc_sql_operation;
	return (*(sop->disconnect_from_db))(dbc);
}

int
dbc_commit_transaction(db_context_t *dbc)
{
	assert(_dbc_info);
	struct dbc_sql_operation_t *sop = _dbc_info->dbc_sql_operation;
	return (*(sop->commit_transaction))(dbc);
}

int
dbc_rollback_transaction(db_context_t *dbc)
{
	assert(_dbc_info);
	struct dbc_sql_operation_t *sop = _dbc_info->dbc_sql_operation;

	/* If disconnected, return ERROR, let caller reconnect to db */
	if (dbc->need_reconnect)
	  return ERROR;

	return (*(sop->rollback_transaction))(dbc);
}

/*
 * if sql_result is NULL mean discard result, dbc driver should
 * release result alloc'd memory. otherwise released in
 * dbc_sql_close_cursor()
 * if execute failed, memory should release in this function.
 */
int
dbc_sql_execute(db_context_t *dbc, char * query, struct sql_result_t * sql_result,
                       char * query_name)
{
	assert(_dbc_info);
	struct dbc_sql_operation_t *sop = _dbc_info->dbc_sql_operation;
	return (*(sop->sql_execute))(dbc, query, sql_result, query_name);
}

void *
dbc_sql_prepare(db_context_t *dbc,  char *query, void *query_name)
{
	assert(_dbc_info);
	struct dbc_sql_operation_t *sop = _dbc_info->dbc_sql_operation;
	return (*(sop->sql_prepare))(dbc, query, query_name);
}

int
dbc_sql_execute_prepared(
	db_context_t *dbc,
	char **params, int num_params, struct sql_result_t *sql_result,
	void *query_stmt)
{
	assert(_dbc_info);
	struct dbc_sql_operation_t *sop = _dbc_info->dbc_sql_operation;
	return (*(sop->sql_execute_prepared))(dbc, params, num_params, sql_result, query_stmt);
}

int
dbc_sql_fetchrow(db_context_t *dbc, struct sql_result_t * sql_result)
{
	assert(_dbc_info);
	struct dbc_sql_operation_t *sop = _dbc_info->dbc_sql_operation;
	return (*(sop->sql_fetchrow))(dbc, sql_result);
}

int
dbc_sql_close_cursor(db_context_t *dbc, struct sql_result_t * sql_result)
{
	assert(_dbc_info);
	struct dbc_sql_operation_t *sop = _dbc_info->dbc_sql_operation;
	return (*(sop->sql_close_cursor))(dbc, sql_result);
}

char *
dbc_sql_getvalue(db_context_t *dbc, struct sql_result_t * sql_result, int field)
{
	assert(_dbc_info);
	struct dbc_sql_operation_t *sop = _dbc_info->dbc_sql_operation;
	return (*(sop->sql_getvalue))(dbc, sql_result, field);
}

/* storeproc */
int dbc_execsp_integrity (db_context_t *dbc, union transaction_data_t *data)
{
	assert(_dbc_info);
	struct dbc_storeproc_operation_t *sop = _dbc_info->dbc_storeproc_operation;
	assert(sop);
	return (*(sop->exec_sp_integrity))(dbc, &data->integrity);
}

int dbc_execsp_delivery (db_context_t *dbc, union transaction_data_t *data)
{
	assert(_dbc_info);
	struct dbc_storeproc_operation_t *sop = _dbc_info->dbc_storeproc_operation;
	assert(sop);
	return (*(sop->exec_sp_delivery))(dbc, &data->delivery);
}

int dbc_execsp_new_order (db_context_t *dbc, union transaction_data_t *data)
{
	assert(_dbc_info);
	struct dbc_storeproc_operation_t *sop = _dbc_info->dbc_storeproc_operation;
	assert(sop);
	return (*(sop->exec_sp_new_order))(dbc, &data->new_order);
}

int dbc_execsp_order_status (db_context_t *dbc, union transaction_data_t *data)
{
	assert(_dbc_info);
	struct dbc_storeproc_operation_t *sop = _dbc_info->dbc_storeproc_operation;
	assert(sop);
	return (*(sop->exec_sp_order_status))(dbc, &data->order_status);
}

int dbc_execsp_payment (db_context_t *dbc, union transaction_data_t *data)
{
	assert(_dbc_info);
	struct dbc_storeproc_operation_t *sop = _dbc_info->dbc_storeproc_operation;
	assert(sop);
	return (*(sop->exec_sp_payment))(dbc, &data->payment);
}

int dbc_execsp_stock_level (db_context_t *dbc, union transaction_data_t *data)
{
	assert(_dbc_info);
	struct dbc_storeproc_operation_t *sop = _dbc_info->dbc_storeproc_operation;
	assert(sop);
	return (*(sop->exec_sp_stock_level))(dbc, &data->stock_level);
}

struct loader_stream_t *dbc_open_loader_stream(db_context_t *dbc,
											   const char *table_name, char delimiter, char *null_str)
{
  assert(_dbc_info);
  struct dbc_loader_operation_t *lop = _dbc_info->dbc_loader_operation;
  assert(lop);
  return (*(lop->open_loader_stream))(dbc, table_name, delimiter, null_str);
}

int dbc_write_to_loader_stream(struct loader_stream_t *stream, const char *fmt, va_list ap)
{
  assert(_dbc_info);
  struct dbc_loader_operation_t *lop = _dbc_info->dbc_loader_operation;
  assert(lop);

  return (*(lop->write_to_stream))(stream, fmt, ap);
}

void dbc_close_loader_stream(struct loader_stream_t *stream)
{
	assert(_dbc_info);
	struct dbc_loader_operation_t *lop = _dbc_info->dbc_loader_operation;
	assert(lop);
	(*(lop->close_loader_stream))(stream);
}


struct sqlapi_operation_t storeproc_sqlapi_operation =
{
	{NULL},
	{
		dbc_execsp_delivery,
		dbc_execsp_new_order,
		dbc_execsp_order_status,
		dbc_execsp_payment,
		dbc_execsp_stock_level,
		dbc_execsp_integrity
	},
	{NULL}
};
