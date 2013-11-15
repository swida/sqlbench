/*
 * This file is released under the terms of the Artistic License.
 * Please see the file LICENSE, included in this package, for details.
 *
 * Copyright (C) 2013 Wang Diancheng
 */
#include <stdio.h>
#include <limits.h>
#include <getopt.h>
#include <string.h>
#include <stdlib.h>
#include <stdint.h>
#include <unistd.h>
#include <errno.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>
#include <sys/stat.h>
#include <fcntl.h>

#define START_MARK 'S'
/* see common.c */
const char transaction_short_name[] = { 'd', 'n', 'o', 'p', 's' };
const char transaction_name[][14] =
		{ "Delivery", "New Order", "Order Status", "Payment", "Stock Level" };

#define TRANSACTION_MAX sizeof(transaction_short_name) / sizeof(transaction_short_name[0])
#define SAMPLE_LENGTH 60
#define DEFAULT_RESPONSE_LENGTH 128
#define DEFAULT_PORT "8237"

FILE *fp_mix_log = NULL;
int fd_output = -1;
int output_binary = 0;

/* timestamp,transaction type,transaction status,response time */
#define split_mixlog_line(tim,ttype,tstatus,rt) \
	do{ \
		char *field = strtok(mix_log_line, ","); \
		int k = 0; \
		tim = atoi(field); \
		ttype = '\0'; \
		while((field = strtok(NULL, ","))) \
		{ \
			switch(k) \
			{ \
				case 0: \
					ttype = field[0]; \
					break; \
				case 1: \
					tstatus = field[0]; \
					break; \
				case 2: \
					rt = atof(field); \
					break; \
				case 3: /* terminal id */ \
					break; \
			} \
			if(k == 3) \
				break; \
			k++; \
		} \
	} while(0)

struct transaction_stat_data
{
	long count[TRANSACTION_MAX]; /* count of every transaction */
	long rollback_count[TRANSACTION_MAX]; /* rollback count of every transaction */
	double response_sum[TRANSACTION_MAX]; /* for time of response calculation  */
	int response_length[TRANSACTION_MAX];
	long unknown_error_count;
	double *response[TRANSACTION_MAX]; 	/* calculate 90th percentile */
	int rate[TRANSACTION_MAX][2];		/* for tpmC sample */
	int rate_sample[TRANSACTION_MAX][SAMPLE_LENGTH];
	int forget_rate_sample[TRANSACTION_MAX];
	unsigned int timestamp, last_timestamp, start_time, end_time;
};

void usage(const char* progname)
{
	printf("usage: %s [-l mix.log] [-n] [-t transaction-rate.log]\n", progname);
	printf("-l mix.log\n");
	printf("\tmix log file, use stdin if not be given\n");
	printf("-t transaction-rate.log\n");
	printf("\tgenerate transacation rate file\n");
	printf("-u [host:port]\n");
	printf("\tsend sample data to a remote process through TCP.\n");
	printf("-n\n");
	printf("\tnot scan start mark\n");
	printf("-h\n");
	printf("\tprint this usage\n");
}

/*
 * 90th percentage calculation by qsort.
 */
int response_time_compare(const void *p1, const void *p2)
{
	if (*(double *)p1 < *(double *)p2)
		return -1;
	else if (*(double *)p1 == *(double *)p2)
		return 0;
	else
		return 1;
}

void
output_final_stat(struct transaction_stat_data *ts)
{
	int i;
	long total_transaction_count = 0;

	ts->end_time = ts->timestamp;

	/* calculate 90th percentile */
	for(i = 0; i < TRANSACTION_MAX; i++)
		qsort(ts->response[i], ts->count[i],
			  sizeof(double), response_time_compare);

	for(i = 0; i < TRANSACTION_MAX; i++)
		total_transaction_count += ts->count[i];

	printf("                         Response Time (s)\n");
	printf(" Transaction      %%    Average :    90th %%        Total        Rollbacks      %%\n");
	printf("------------  -----  ---------------------  -----------  ---------------  -----\n");

	for(i = 0; i < TRANSACTION_MAX; i++)
	printf("%12s  %5.2f  %9.3f : %9.3f  %11ld  %15ld  %5.2f\n",
		   transaction_name[i],
		   total_transaction_count == 0 ? 0.0 : (double)ts->count[i] / (double)total_transaction_count * 100.0,
		   ts->count[i] == 0 ? 0.0 : ts->response_sum[i] / (double)ts->count[i] ,
		   ts->response[i][(int)(0.9 * ts->count[i])], ts->count[i],
		   ts->rollback_count[i],
		   ts->rollback_count[i] == 0 ? 0.0 : (double)ts->rollback_count[i] / (double)ts->count[i] * 100.0);

	printf("------------  -----  ---------------------  -----------  ---------------  -----\n\n");
	printf("%0.2f new-order transactions per minute (NOTPM)\n", (double)ts->count[1] / (double)(ts->end_time - ts->start_time) * 60.0);
	printf("%0.1f minute duration\n", (double)(ts->end_time - ts->start_time) / 60.0);
	printf("%ld total unknown errors\n", ts->unknown_error_count);
	printf("%d second(s) ramping up\n", ts->start_time);
}

void
sample_transaction_rate(struct transaction_stat_data *ts, int ttype_idx)
{
	int t;
	static int j = 0;
	char buf[256];
	unsigned int nlval;

	if(ts->last_timestamp == UINT32_MAX)
		ts->last_timestamp = ts->timestamp;

	ts->rate_sample[ttype_idx][j]++;

	if(ts->last_timestamp != ts->timestamp)
	{
		int len;
		if(output_binary)
		{
			nlval = htonl(ts->last_timestamp);
			memcpy(buf, &nlval, sizeof(nlval));
			len = sizeof(nlval);
		}else
			len = sprintf(buf, "%u", ts->last_timestamp);

		for(t = 0; t < TRANSACTION_MAX; t++)
		{
			ts->rate[t][1] = ts->rate[t][0] +
				ts->rate_sample[t][j] - ts->forget_rate_sample[t];
			ts->rate[t][0] = ts->rate[t][1];
			if(output_binary)
			{
				nlval = htonl(ts->rate[t][1]);
				memcpy(buf + len, &nlval, sizeof(nlval));
				len += sizeof(nlval);
			}
			else
				len += sprintf(buf + len, " %d", ts->rate[t][1]);
		}
		if(!output_binary)
			buf[len++] = '\n';
		write(fd_output, buf, len);
		j++;

		j %= SAMPLE_LENGTH;
		for(t = 0; t < TRANSACTION_MAX; t++)
		{
			ts->forget_rate_sample[t] = ts->rate_sample[t][j];
			ts->rate_sample[t][j] = 0;
		}
		ts->last_timestamp = ts->timestamp;
	}
}

void
roll_file_stat(int noseek_start_mark, struct transaction_stat_data *ts)
{
	int i;
	char mix_log_line[32];
	static int start = 0;

	while(fgets(mix_log_line, 32, fp_mix_log))
	{
		char transaction_type;
		char transaction_status;
		double response_time;

		split_mixlog_line(
			ts->timestamp, transaction_type,
			transaction_status, response_time);

		if(transaction_type == '\0')				/* invalid line */
			continue;

		for (i = 0; i < TRANSACTION_MAX; i++)
			if(transaction_type == transaction_short_name[i])
				break;

		/* transaction rate, print transaction type title, too */
		if( fd_output != -1 && i < TRANSACTION_MAX)
			sample_transaction_rate(ts, i);

		/* start mark not found, mix log line is not in count if
		 * noseek_start_mark not set */
		if(start == 0)
		{
			if(transaction_type == START_MARK || noseek_start_mark)
			{
				start = 1;
				ts->start_time = ts->timestamp;
			}
			continue;
		}

		if (i >= TRANSACTION_MAX)
			continue;

		ts->response[i][ts->count[i]] = response_time;
		/* enlarge response array buffer */
		if(ts->count[i] >= ts->response_length[i])
		{
			ts->response_length[i] = ts->response_length[i] * 2;
			void *p = realloc(ts->response[i],
							  ts->response_length[i] * sizeof(double));
			if (!p)
			{
				printf("out of memory\n");
				exit(1);
			}
			ts->response[i] = p;
		}
		ts->count[i]++;
		ts->response_sum[i] += response_time;
		if(transaction_status == 'R')
			ts->rollback_count[i]++;
		else if(transaction_status == 'E')
			ts->unknown_error_count++;
	}
}

int
connect_remote_host(const char *host, const char *port)
{
	struct addrinfo hints;
	struct addrinfo *result, *rp;
	int sfd, s;

	/* Obtain address(es) matching host/port */
	memset(&hints, 0, sizeof(struct addrinfo));
	hints.ai_family = AF_UNSPEC;    /* Allow IPv4 or IPv6 */
	hints.ai_socktype = SOCK_STREAM; /* Datagram socket */
	hints.ai_flags = 0;
	hints.ai_protocol = 0;          /* Any protocol */
	s = getaddrinfo(host, port, &hints, &result);

	if (s != 0) {
		fprintf(stderr, "getaddrinfo: %s\n", gai_strerror(s));
		return -1;
	}

	for (rp = result; rp != NULL; rp = rp->ai_next) {
		sfd = socket(rp->ai_family, rp->ai_socktype,
					 rp->ai_protocol);
		if (sfd == -1)
			continue;

		if (connect(sfd, rp->ai_addr, rp->ai_addrlen) != -1)
			break;                  /* Success */

		close(sfd);
	}

	if (rp == NULL) {               /* No address succeeded */
		fprintf(stderr, "Could not connect\n");
		return -1;
	}
	return sfd;
}

int main(int argc, char *argv[])
{
	int i;
	char mix_log_file[PATH_MAX] = "";
	char transaction_rate_file[PATH_MAX] = "";
	int noseek_start_mark = 0;
	char host[64] = "";
	char port[8] = DEFAULT_PORT;
	int follow_mode = 0;

	struct transaction_stat_data ts;
	memset(&ts, 0, sizeof(ts));

	while (1) {
		int option_index = 0;
		struct option long_options[] = {{0, 0, 0, 0}};
		int c = getopt_long(argc, argv, "hl:nt:u:",
			long_options, &option_index);
		if (c == -1) {
			break;
		}
		switch(c)
		{
			case 'h':
				usage(argv[0]);
				return 0;
			case 'l':
				strncpy(mix_log_file, optarg, PATH_MAX);
				break;
			case 'n':
				noseek_start_mark = 1;
				break;
			case 't':
				strncpy(transaction_rate_file, optarg, PATH_MAX);
				break;
			case 'u':
				{
					char *pt = strchr(optarg, ':');
					if(pt)
					{
						strncpy(host, optarg, pt - optarg < 64 ? pt - optarg : 64);
						strncpy(port, pt + 1, 8);
					}
					else
						strncpy(host, optarg, 64);
				}
				break;
			default:
				usage(argv[0]);
				return 1;
		}
	}

	if(transaction_rate_file[0] != '\0' && host[0] != '\0')
	{
		fprintf(stderr, "conflicting option string(s): -u and -t\n");
		return 1;
	}

	if(mix_log_file[0] == '\0')
		fp_mix_log = stdin;
	else
		fp_mix_log = fopen(mix_log_file, "r");

	if(!fp_mix_log)
	{
		perror("could not open mix log file");
		return 1;
	}

	if(transaction_rate_file[0] != '\0')
	{
		if(strcmp(transaction_rate_file, "-") == 0)
		{
			fd_output = 1;
			follow_mode = 1;
		}
		else
			fd_output = open(transaction_rate_file, O_CREAT | O_WRONLY,
							 S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP | S_IROTH | S_IWOTH);
		if(fd_output == -1)
		{
			perror("could not open transaction rate file");
			return 1;
		}
	} else if(host[0] != '\0')
	{
		fd_output = connect_remote_host(host, port);
		if (fd_output == -1)
			return 1;
		follow_mode = 1;
		output_binary = 1;
	}

	/* initialize response time array */
	for(i = 0; i < TRANSACTION_MAX; i++)
	{
		ts.response_length[i] = DEFAULT_RESPONSE_LENGTH;
		ts.response[i] = malloc(ts.response_length[i] * sizeof(double));
	}
	ts.last_timestamp = UINT32_MAX;

	errno = 0;
	do{
		roll_file_stat(noseek_start_mark, &ts);
		if(follow_mode)
			usleep(250000);
	}while(follow_mode && errno == 0);

	if(argc < 2)
		fclose(fp_mix_log);

	if(fd_output != -1)
		close(fd_output);

	output_final_stat(&ts);

	for(i = 0; i < TRANSACTION_MAX; i++)
		free(ts.response[i]);

	return 0;
}
