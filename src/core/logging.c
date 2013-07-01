/*
 * This file is released under the terms of the Artistic License.  Please see
 * the file LICENSE, included in this package, for details.
 *
 * Copyright (C) 2002 Mark Wong & Open Source Development Labs, Inc.
 *
 * 19 August 2002
 */

#include <common.h>
#include <pthread.h>
#include <stdio.h>
#include <stdarg.h>
#include <string.h>
#include <time.h>
#include <logging.h>
#include <transaction_data.h>

FILE *log_error;
pthread_mutex_t mutex_error_log = PTHREAD_MUTEX_INITIALIZER;

int edump(int type, void *data)
{
	pthread_mutex_lock(&mutex_error_log);
	fprintf(log_error, "[%d]\n", (int) pthread_self());
	dump(log_error, type, data);
	pthread_mutex_unlock(&mutex_error_log);

	return OK;
}

int init_logging()
{
	char log_filename[1024];
	/* Open a file to log errors to. */
	if (output_path[0] != '\0') {
		strcat(output_path, "/");
	}
	sprintf(log_filename, "%s%s", output_path, ERROR_LOG_NAME);
	log_error = fopen(log_filename, "w");
	if (log_error == NULL) {
		fprintf(stderr, "cannot open %s\n", log_filename);
		return ERROR;
	}

	return OK;
}

int log_error_message(char *filename, int line, const char *fmt, ...)
{
	va_list fmtargs;
	time_t t;
	FILE *of = (log_error) ? log_error: stderr;

	/* Print the error message(s) */
	t = time(NULL);
	va_start(fmtargs, fmt);

	pthread_mutex_lock(&mutex_error_log);
	fprintf(of, "%stid:%d %s:%d\n", ctime(&t), (int) pthread_self(),
		filename, line);
	va_start(fmtargs, fmt);
	vfprintf(of, fmt, fmtargs);
	va_end(fmtargs);
	fprintf(of, "\n");
	fflush(log_error);
	pthread_mutex_unlock(&mutex_error_log);

	return OK;
}
