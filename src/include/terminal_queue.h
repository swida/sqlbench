#ifndef TERMINAL_QUEUE_H
#define TERMINAL_QUEUE_H
#include <semaphore.h>
#include "transaction_queue.h"

typedef unsigned short tqueue_t;

struct terminal_t
{
	int id;
	unsigned int running_time;
	struct transaction_queue_node_t node;
};

struct terminal_queue_t
{
	tqueue_t *queue;
	int length;
	unsigned int timing;
	struct terminal_t *terminals;
	pthread_mutex_t queue_mutex;
	sem_t queue_length;
};

struct termworker_context_t
{
	int id;
	int start_term;
	int end_term;
	struct terminal_queue_t term_queue;
};

char generate_transaction(int *keying_time, int *mean_think_time);
void enqueue_terminal(int termworker_id, int term);
struct terminal_t *dequeue_terminal(int termworker_id, unsigned int *delay);
void init_termworker_array(int thread_count);
void destroy_termworker_array(int thread_count);
struct  termworker_context_t *init_termworker_context(int id, int thread_count);
#endif
