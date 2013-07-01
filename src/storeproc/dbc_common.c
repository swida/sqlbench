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

#include "common.h"
#include "logging.h"
#include "libpq_common.h"

char dbname[32] = "dbt2";
char pghost[32] = "localhost";
char pgport[32] = "5432";

int commit_transaction(struct db_context_t *dbc)
{
        PGresult *res;

        res = PQexec(dbc->conn, "COMMIT");
        if (!res || PQresultStatus(res) != PGRES_COMMAND_OK) {
                LOG_ERROR_MESSAGE("%s", PQerrorMessage(dbc->conn));
        }
        PQclear(res);

        return OK;
}

/* Open a connection to the database. */
int _connect_to_db(struct db_context_t *dbc)
{
        char buf[1024];
        if(strcmp(pghost, "localhost") == 0 &&
           strcmp(pgport, "5432") == 0) {
                sprintf(buf, "dbname=%s", dbname);
        } else {
                sprintf(buf, "host=%s port=%s dbname=%s", pghost, pgport, dbname);
        }
        dbc->conn = PQconnectdb(buf);
        if (PQstatus(dbc->conn) != CONNECTION_OK) {
                LOG_ERROR_MESSAGE("Connection to database '%s' failed.",
                        dbname);
                LOG_ERROR_MESSAGE("%s", PQerrorMessage(dbc->conn));
                PQfinish(dbc->conn);
                return ERROR;
        }
        return OK;
}

/* Disconnect from the database and free the connection handle. */
int _disconnect_from_db(struct db_context_t *dbc)
{
        PQfinish(dbc->conn);
        return OK;
}

int _db_init(char *_dbname, char *_pghost, char *_pgport)
{
        /* Copy values only if it's not NULL. */
        if (_dbname != NULL) {
                strcpy(dbname, _dbname);
        }
        if (_pghost != NULL) {
                strcpy(pghost, _pghost);
        }
        if (_pgport != NULL) {
                strcpy(pgport, _pgport);
        }
        return OK;
}

int rollback_transaction(struct db_context_t *dbc)
{
        PGresult *res;

        res = PQexec(dbc->conn, "ROLLBACK");
        if (!res || PQresultStatus(res) != PGRES_COMMAND_OK) {
                LOG_ERROR_MESSAGE("%s", PQerrorMessage(dbc->conn));
        }
        PQclear(res);

        return STATUS_ROLLBACK;
}
