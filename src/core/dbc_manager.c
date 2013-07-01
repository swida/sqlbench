#include "dbc.h"
#include "common.h"
#include <string.h>

extern int pgsql_dbc_init();

struct dbc_construct_t dbc_constructs[] =
{
	{pgsql_dbc_init},
	NULL
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
static char output_dbc_usages[128] = "";

/* user final selected dbc */
static struct dbc_info_t *_dbc_info = NULL;

struct dbc_info_t *make_dbc_info(const char *dbcname, const char *dbcusage)
{
	struct dbc_info_t *info = malloc(sizeof(struct dbc_info_t));
	info->dbc_name = (char *)dbcname;
	info->dbc_usage = (char *)dbcusage;

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
		sprintf(output_dbc_names, "%s %s", output_dbc_names, curr_desc->dbc_info->dbc_name);
		sprintf(output_dbc_usages, "%s\t%s\n", output_dbc_usages, curr_desc->dbc_info->dbc_usage);
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

/* interfaces */
struct db_context_t *
dbc_db_init(void)
{
	assert(_dbc_info);
	struct dbc_sql_operation_t *sop = _dbc_info->dbc_sql_operation;
	return (*(sop->db_init))();
}
int
dbc_connect_to_db(struct db_context_t *dbc)
{
	assert(_dbc_info);
	struct dbc_sql_operation_t *sop = _dbc_info->dbc_sql_operation;
	return (*(sop->connect_to_db))(dbc);
}

int
dbc_disconnect_from_db(struct db_context_t *dbc)
{
	assert(_dbc_info);
	struct dbc_sql_operation_t *sop = _dbc_info->dbc_sql_operation;
	return (*(sop->disconnect_from_db))(dbc);
}

int
dbc_commit_transaction(struct db_context_t *dbc)
{
	assert(_dbc_info);
	struct dbc_sql_operation_t *sop = _dbc_info->dbc_sql_operation;
	return (*(sop->commit_transaction))(dbc);
}

int
dbc_rollback_transaction(struct db_context_t *dbc)
{
	assert(_dbc_info);
	struct dbc_sql_operation_t *sop = _dbc_info->dbc_sql_operation;
	return (*(sop->rollback_transaction))(dbc);
}

int
dbc_sql_execute(struct db_context_t *dbc, char * query, struct sql_result_t * sql_result,
                       char * query_name)
{
	assert(_dbc_info);
	struct dbc_sql_operation_t *sop = _dbc_info->dbc_sql_operation;
	return (*(sop->sql_execute))(dbc, query, sql_result, query_name);
}

int
dbc_sql_fetchrow(struct db_context_t *dbc, struct sql_result_t * sql_result)
{
	assert(_dbc_info);
	struct dbc_sql_operation_t *sop = _dbc_info->dbc_sql_operation;
	return (*(sop->sql_fetchrow))(dbc, sql_result);
}

int
dbc_sql_close_cursor(struct db_context_t *dbc, struct sql_result_t * sql_result)
{
	assert(_dbc_info);
	struct dbc_sql_operation_t *sop = _dbc_info->dbc_sql_operation;
	return (*(sop->sql_close_cursor))(dbc, sql_result);
}

char *
dbc_sql_getvalue(struct db_context_t *dbc, struct sql_result_t * sql_result, int field)
{
	assert(_dbc_info);
	struct dbc_sql_operation_t *sop = _dbc_info->dbc_sql_operation;
	return (*(sop->sql_getvalue))(dbc, sql_result, field);
}
