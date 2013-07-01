/*
 * This file is released under the terms of the Artistic License.  Please see
 * the file LICENSE, included in this package, for details.
 *
 * Copyright (C) 2002 Mark Wong & Open Source Development Lab, Inc.
 *
 * 5 august 2002
 */

#include <common.h>
#include <logging.h>
#include <client_interface.h>
#include <_socket.h>

int connect_to_client(char *addr, int port)
{
	int sockfd;

	sockfd = _connect(addr, port);

	return sockfd;
}

int receive_transaction_data(int s, struct client_transaction_t *client_data)
{
	int length;

	length = _receive(s, client_data, sizeof(struct client_transaction_t));
	if (length == -1) {
		LOG_ERROR_MESSAGE("cannot receive interaction data");
		return ERROR;
	} else if (length == 0) {
		LOG_ERROR_MESSAGE("socket closed on _receive");
		return ERROR_SOCKET_CLOSED;
	} else if (length != sizeof(struct client_transaction_t)) {
		LOG_ERROR_MESSAGE("did not receive all data");
		return ERROR;
	}

	return length;
}

int send_transaction_data(int s, struct client_transaction_t *client_data)
{
	int length;

	length = _send(s, (void *) client_data,
		sizeof(struct client_transaction_t));
	if (length == -1) {
		LOG_ERROR_MESSAGE("cannot send transaction data");
		return ERROR;
	} else if (length == 0) {
		LOG_ERROR_MESSAGE("socket closed on _send");
		return ERROR_SOCKET_CLOSED;
	} else if (length != sizeof(struct client_transaction_t)) {
		LOG_ERROR_MESSAGE("did not send all data");
		return ERROR;
	}

	return length;
}
