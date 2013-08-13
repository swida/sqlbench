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

#define START_MARK 'S'
/* see common.c */
const char transaction_short_name[] = { 'd', 'n', 'o', 'p', 's' };
const char transaction_name[][14] =
		{ "Delivery", "New Order", "Order Status", "Payment", "Stock Level" };

#define TRANSACTION_MAX sizeof(transaction_short_name) / sizeof(transaction_short_name[0])
#define SAMPLE_LENGTH 60

void usage(const char* progname)
{
	printf("usage: %s [-f mix.log] [-n] [-t transaction-rate.log]\n", progname);
	printf("-f mix.log\n");
	printf("\tmix log file, use stdin if not be given\n");
	printf("-t transaction-rate.log\n");
	printf("\tgenerate transacation rate file\n");
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

int main(int argc, char *argv[])
{
	FILE *fp, *trfp = NULL;
	int i, j;
	long total_transaction_count = 0;
	long transaction_count[] = {0, 0, 0, 0, 0};
	long transaction_rollback_count[] = {0, 0, 0, 0, 0};
	double transaction_response_sum[] = {0.0, 0.0, 0.0, 0.0, 0.0};
	double *transaction_response[TRANSACTION_MAX];
	int transaction_response_length[] = {128, 128, 128, 128, 128};
	int transaction_rate[TRANSACTION_MAX][2];
	int transaction_rate_sample[TRANSACTION_MAX][SAMPLE_LENGTH];
	int forget_rate_sample[TRANSACTION_MAX];
	unsigned int timestamp, last_timestamp, start_time, end_time;
	long unknown_error_count = 0;
	char mix_log_file[PATH_MAX] = "";
	char transaction_rate_file[PATH_MAX] = "";
	char mix_log_line[32];
	int start = 0;
	int noseek_start_mark = 0;

	while (1) {
		int option_index = 0;
		struct option long_options[] = {{0, 0, 0, 0}};
		int c = getopt_long(argc, argv, "f:hnt:",
			long_options, &option_index);
		if (c == -1) {
			break;
		}
		switch(c)
		{
			case 'f':
				strncpy(mix_log_file, optarg, PATH_MAX);
				break;
			case 'h':
				usage(argv[0]);
				return 0;
			case 'n':
				noseek_start_mark = 1;
				break;
			case 't':
				strncpy(transaction_rate_file, optarg, PATH_MAX);
				break;
			default:
				usage(argv[0]);
				return 1;
		}
			
	}
	if(mix_log_file[0] == '\0')
		fp = stdin;
	else
		fp = fopen(mix_log_file, "r");

	if(!fp)
	{
		perror("could not open mix log file");
		return 1;
	}

	if(transaction_rate_file[0] != '\0')
	{
		trfp = fopen(transaction_rate_file, "w");
		if(!trfp)
		{
			perror("could not open transaction rate file");
			return 1;
		}
		for(i = 0; i < TRANSACTION_MAX; i++)
		{
			memset(transaction_rate_sample[i], 0, SAMPLE_LENGTH * sizeof(int));
			transaction_rate[i][0] = transaction_rate[i][1] = 0;
			forget_rate_sample[i] = 0;
		}
		j = 0;
		last_timestamp = UINT32_MAX;
	}

	/* initialize response time array */
	for(i = 0; i < TRANSACTION_MAX; i++)
		transaction_response[i] = malloc(transaction_response_length[i] * sizeof(double));

	while(fgets(mix_log_line, 32, fp))
	{
		char transaction_type;
		char transaction_status;
		double response_time;
		int terminal_id;

		char *field = strtok(mix_log_line, ",");
		timestamp = atoi(field);
		i = 0;
		transaction_type = '\0';
		while((field = strtok(NULL, ",")))
		{
			switch(i)
			{
				case 0:
					transaction_type = field[0];
					break;
				case 1:
					transaction_status = field[0];
					break;
				case 2:
					response_time = atof(field);
					break;
				case 3:
					terminal_id = atoi(field);
					(void)terminal_id;
					break;
			}
			if(i == 3)
				break;
			i++;
		}

		if(transaction_type == '\0')				/* invalid line */
			continue;

		for (i = 0; i < TRANSACTION_MAX; i++)
			if(transaction_type == transaction_short_name[i])
				break;

		/* transaction rate, print transaction type title, too */
		if(transaction_rate_file[0] != '\0' && i < TRANSACTION_MAX)
		{
			int t;

			if(last_timestamp == UINT32_MAX)
			{
				fprintf(trfp, "# time");
				for(t = 0; t < TRANSACTION_MAX; t++)
					fprintf(trfp, " \"%s\"", transaction_name[t]);
				fprintf(trfp, "\n");
				last_timestamp = timestamp;
			}

			transaction_rate_sample[i][j]++;

			if(last_timestamp != timestamp)
			{
				fprintf(trfp, "%u", last_timestamp);						
				for(t = 0; t < TRANSACTION_MAX; t++)
				{
					transaction_rate[t][1] = transaction_rate[t][0] +
						transaction_rate_sample[t][j] - forget_rate_sample[t];
					transaction_rate[t][0] = transaction_rate[t][1];
					fprintf(trfp, " %d", transaction_rate[t][1]);
				}

				fprintf(trfp, "\n");
				j++;

				j %= SAMPLE_LENGTH;
				for(t = 0; t < TRANSACTION_MAX; t++)
				{
					forget_rate_sample[t] = transaction_rate_sample[t][j];
					transaction_rate_sample[t][j] = 0;
				}
				last_timestamp = timestamp;
			}
		}
		if(start == 0)
		{
			if(transaction_type == START_MARK || noseek_start_mark)
			{
				start = 1;
				start_time = timestamp;
			}
			continue;
		}

		if (i >= TRANSACTION_MAX)
			continue;

		transaction_response[i][transaction_count[i]] = response_time;
		/* enlarge response array buffer */
		if(transaction_count[i] >= transaction_response_length[i])
		{
			transaction_response_length[i] = transaction_response_length[i] * 2;
			void *p = realloc(transaction_response[i],
							  transaction_response_length[i] * sizeof(double));
			if (!p)
			{
				printf("out of memory\n");
				return 1;
			}
			transaction_response[i] = p;
		}
		transaction_count[i]++;
		transaction_response_sum[i] += response_time;
		if(transaction_status == 'R')
			transaction_rollback_count[i]++;
		else if(transaction_status == 'E')
			unknown_error_count++;
	}
	end_time = timestamp;

	/* calculate 90th percentile */
	for(i = 0; i < TRANSACTION_MAX; i++)
		qsort(transaction_response[i], transaction_count[i],
			  sizeof(double), response_time_compare);

	if(argc < 2)
		fclose(fp);
	if(transaction_rate_file[0] != '\0')
		fclose(trfp);

	for(i = 0; i < TRANSACTION_MAX; i++)
		total_transaction_count += transaction_count[i];

	printf("                         Response Time (s)\n");
	printf(" Transaction      %%    Average :    90th %%        Total        Rollbacks      %%\n");
	printf("------------  -----  ---------------------  -----------  ---------------  -----\n");

	for(i = 0; i < TRANSACTION_MAX; i++)
	printf("%12s  %5.2f  %9.3f : %9.3f  %11ld  %15ld  %5.2f\n",
		   transaction_name[i],
		   total_transaction_count == 0 ? 0.0 : (double)transaction_count[i] / (double)total_transaction_count * 100.0,
		   transaction_count[i] == 0 ? 0.0 : transaction_response_sum[i] / (double)transaction_count[i] ,
		   transaction_response[i][(int)(0.9 * transaction_count[i])], transaction_count[i],
		   transaction_rollback_count[i],
		   transaction_rollback_count[i] == 0 ? 0.0 : (double)transaction_rollback_count[i] / (double)transaction_count[i] * 100.0);

	printf("------------  -----  ---------------------  -----------  ---------------  -----\n\n");
	printf("%0.2f new-order transactions per minute (NOTPM)\n", (double)transaction_count[1] / (double)(end_time - start_time) * 60.0);
	printf("%0.1f minute duration\n", (double)(end_time - start_time) / 60.0);
	printf("%ld total unknown errors\n", unknown_error_count);
	printf("%d second(s) ramping up\n", start_time);

	for(i = 0; i < TRANSACTION_MAX; i++)
		free(transaction_response[i]);

	return 0;
}
