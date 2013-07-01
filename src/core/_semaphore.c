/*
 * _semaphore.c
 *
 * This file is released under the terms of the Artistic License. Please see
 * the file LICENSE, included in this package, for details.
 *
 * Copyright (C) 2002 Sergey Golubchik & MySQL AB
 *
 * Simple implementation of semaphores, needed to compile test on systems
 * that doesn't support semaphores.
 */

#include <_semaphore.h>
#include <errno.h>

#if !defined(HAVE_SEMAPHORE_H) || defined(BROKEN_SEMAPHORES)

int sem_init(sem_t * sem, int pshared, unsigned int value)
{
  sem->count=value;
  pthread_cond_init(&sem->cond, 0);
  pthread_mutex_init(&sem->mutex, 0);
  return 0;
}

int sem_destroy(sem_t * sem)
{
  int err1,err2;
  err1=pthread_cond_destroy(&sem->cond);
  err2=pthread_mutex_destroy(&sem->mutex);
  if (err1 || err2)
  {
    errno=err1 ? err1 : err2;
    return -1;
  }
  return 0;
}

int sem_wait(sem_t * sem)
{
  if ((errno=pthread_mutex_lock(&sem->mutex)))
    return -1;
  while (!sem->count)
    pthread_cond_wait(&sem->cond, &sem->mutex);
  if (errno)
    return -1;
  sem->count--; /* mutex is locked here */
  pthread_mutex_unlock(&sem->mutex);
  return 0;
}

int sem_trywait(sem_t * sem)
{
  if ((errno=pthread_mutex_lock(&sem->mutex)))
    return -1;
  if (sem->count)
    sem->count--;
  else
    errno=EAGAIN;
  pthread_mutex_unlock(&sem->mutex);
  return errno ? -1 : 0;
}


int sem_post(sem_t * sem)
{
  if ((errno=pthread_mutex_lock(&sem->mutex)))
    return -1;
  sem->count++;
  pthread_mutex_unlock(&sem->mutex);    /* does it really matter what to do */
  pthread_cond_signal(&sem->cond);      /* first: x_unlock or x_signal ?    */
  return 0;
}

int sem_post_multiple(sem_t * sem, unsigned int count)
{
  if ((errno=pthread_mutex_lock(&sem->mutex)))
    return -1;
  sem->count+=count;
  pthread_mutex_unlock(&sem->mutex);    /* does it really matter what to do */
  pthread_cond_broadcast(&sem->cond);   /* first: x_unlock or x_broadcast ? */
  return 0;
}

int sem_getvalue(sem_t * sem, unsigned int *sval)
{
  if ((errno=pthread_mutex_lock(&sem->mutex)))
    return -1;
  *sval=sem->count;
  pthread_mutex_unlock(&sem->mutex);
  return 0;
}

#endif /* !defined(HAVE_SEMAPHORE_H) || defined(BROKEN_SEMAPHORES) */

