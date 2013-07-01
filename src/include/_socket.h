/*
 * _socket.h
 *
 * This file is released under the terms of the Artistic License.  Please see
 * the file LICENSE, included in this package, for details.
 *
 * Copyright (C) 2002 Mark Wong & Jenny Zhang &
 *                    Open Source Development Lab, Inc.
 *
 * 20 march 2002
 */

#ifndef __SOCKET_H_
#define __SOCKET_H_

#define ERR_SOCKET_CREATE -1
#define ERR_SOCKET_BIND -2
#define ERR_SOCKET_LISTEN -3
#define ERR_SOCKET_RESOLVPROTO -4

#include <netdb.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>

int _accept(int *s);
int _connect(char *address, unsigned short port);
int _receive(int s, void *data, int length);
int _send(int s, void *data, int length);
int _listen(int port);

#endif /* __SOCKET_H_ */
