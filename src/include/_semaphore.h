/*
 * _semaphore.h
 *
 * This file is released under the terms of the Artistic License.  Please see
 * the file LICENSE, included in this package, for details.
 *
 */


#ifndef __semaphore_h_
#define __semaphore_h_

#include <pthread.h>
#include "config.h"

#if defined(HAVE_SEMAPHORE_H) && !defined(BROKEN_SEMAPHORES)
#include <semaphore.h>
#else
typedef struct {
  pthread_mutex_t mutex;
  pthread_cond_t  cond;
  unsigned int            count;
} sem_t;

int sem_init(sem_t * sem, int pshared, unsigned int value);
int sem_destroy(sem_t * sem);
int sem_trywait(sem_t * sem);
int sem_wait(sem_t * sem);
int sem_post(sem_t * sem);
int sem_post_multiple(sem_t * sem, unsigned int count);
int sem_getvalue(sem_t * sem, unsigned int * sval);

#endif /* HAVE_SEMAPHORE_H && !DARWIN */

#endif /* !__semaphore_h_ */
