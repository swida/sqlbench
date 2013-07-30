#include <string.h>
#include "terminal_queue.h"
#include "driver.h"

static struct termworker_context_t *termworker_context;

char generate_transaction(int *keying_time, int *mean_think_time)
{
	double threshold;
	char transaction;

	/*
	 * Determine which transaction to execute, minimum keying time,
	 * and mean think time.
	 */
	threshold = get_percentage();
	if (threshold < transaction_mix.new_order_threshold) {
		transaction = NEW_ORDER;
		*keying_time = key_time.new_order;
		*mean_think_time = think_time.new_order;
	} else if (transaction_mix.payment_actual != 0 &&
			   threshold < transaction_mix.payment_threshold) {
		transaction = PAYMENT;
		*keying_time = key_time.payment;
		*mean_think_time = think_time.payment;
	} else if (transaction_mix.order_status_actual != 0 &&
			   threshold < transaction_mix.order_status_threshold) {
		transaction = ORDER_STATUS;
		*keying_time = key_time.order_status;
		*mean_think_time = think_time.order_status;
	} else if (transaction_mix.delivery_actual != 0 &&
			   threshold < transaction_mix.delivery_threshold) {
		transaction = DELIVERY;
		*keying_time = key_time.delivery;
		*mean_think_time = think_time.delivery;
	} else {
		transaction = STOCK_LEVEL;
		*keying_time = key_time.stock_level;
		*mean_think_time = think_time.stock_level;
	}
	return transaction;
}

void enqueue_terminal(int termworker_id, int term)
{
	char transaction;
	int keying_time;
	int mean_think_time; /* In milliseconds. */
	unsigned int running_time;
	int low, high;

	struct terminal_queue_t *term_queue =
		&termworker_context[termworker_id].term_queue;
	struct terminal_t *terms = term_queue->terminals;

	transaction = generate_transaction(&keying_time, &mean_think_time);

	running_time = keying_time * 1000 + get_think_time(mean_think_time) + term_queue->timing;

	pthread_mutex_lock(&term_queue->queue_mutex);
	/* find insert pos */
	low = 0;
	high = term_queue->length;
	while (high > low)
	{
		int mid = low + (high - low) / 2;
		if (terms[term_queue->queue[mid]].running_time >= running_time)
			low = mid + 1;
		else
			high = mid;
	}

	/* pos found */
	memmove(term_queue->queue + low + 1,
			term_queue->queue + low, (term_queue->length - low) * sizeof(tqueue_t));
	/* stored 0 based position */
	term_queue->queue[low] = (tqueue_t)(term - termworker_context[termworker_id].start_term);
	terms[term_queue->queue[low]].running_time = running_time;
	terms[term_queue->queue[low]].id = term;
	/* so looong */
	terms[term_queue->queue[low]].node.client_data.transaction = transaction;
	term_queue->length++;
	sem_post(&term_queue->queue_length);
	pthread_mutex_unlock(&term_queue->queue_mutex);
}

struct terminal_t *dequeue_terminal(int termworker_id, unsigned int *delay)
{
	struct terminal_t *term;
	struct terminal_queue_t *term_queue =
		&termworker_context[termworker_id].term_queue;
	sem_wait(&term_queue->queue_length);
	pthread_mutex_lock(&term_queue->queue_mutex);
	term_queue->length--;
	term = &term_queue->terminals[term_queue->queue[term_queue->length]];
	*delay = term->running_time - term_queue->timing;
	term_queue->timing = term->running_time;
	pthread_mutex_unlock(&term_queue->queue_mutex);
	return term;
}

void init_termworker_array(int thread_count)
{
	termworker_context = malloc(sizeof(struct termworker_context_t) * thread_count);
}

struct termworker_context_t * init_termworker_context(int id, int thread_count)
{
	int terminal_count;
	struct termworker_context_t *tc = &termworker_context[id];
	tc->id = id;
	tc->start_term = id * terminals_per_thread;
	if(id == thread_count - 1)
		tc->end_term = (w_id_max - w_id_min + 1) * terminals_per_warehouse;
	else
		tc->end_term = (id + 1) * terminals_per_thread;

	/* initialize terminal array */
	if (mode_altered > 0)
	{
		/*
		 * This effectively allows one client to touch
		 * the entire warehouse range.  The setting of
		 * w_id and d_id is moot in this case.
		 */
		tc->start_term = 0;
		tc->end_term = 1;
	}
	terminal_count = tc->end_term - tc->start_term;
	tc->term_queue.queue = malloc(sizeof(tqueue_t) * terminal_count);
	tc->term_queue.terminals = malloc(sizeof(struct terminal_t) * terminal_count);
	tc->term_queue.length = 0;
	tc->term_queue.timing = 0;
	sem_init(&tc->term_queue.queue_length, 0, 0);
	pthread_mutex_init(&tc->term_queue.queue_mutex, NULL);

	return &termworker_context[id];
}

void destroy_termworker_array(int thread_count)
{
	int i;
	for(i = 0; i < thread_count; i++)
	{
		struct termworker_context_t *tc = &termworker_context[i];
		sem_destroy(&tc->term_queue.queue_length);
		pthread_mutex_destroy(&tc->term_queue.queue_mutex);
		free(tc->term_queue.queue);
		free(tc->term_queue.terminals);
	}
}
