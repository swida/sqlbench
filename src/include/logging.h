/*
 * logging.h
 *
 * This file is released under the terms of the Artistic License.  Please see
 * the file LICENSE, included in this package, for details.
 *
 * Copyright (C) 2002 Mark Wong & Open Source Development Lab, Inc.
 *
 * 19 august 2002
 */

#ifndef _LOGGING_H_
#define _LOGGING_H_

#define ERROR_LOG_NAME "error.log"
#define LOG_ERROR_MESSAGE(arg...) log_error_message(__FILE__, __LINE__, ## arg)

int edump(int type, void *data);
int init_logging();
int log_error_message(char *filename, int line, const char *fmt, ...);

#endif /* _LOGGING_H_ */
