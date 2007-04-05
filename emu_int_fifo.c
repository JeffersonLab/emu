/*----------------------------------------------------------------------------*
 *  Copyright (c) 2007        Southeastern Universities Research Association, *
 *                            Thomas Jefferson National Accelerator Facility  *
 *                                                                            *
 *    This software was developed under a United States Government license    *
 *    described in the NOTICE file included as part of this distribution.     *
 *                                                                            *
 *    Author:  Graham Heyes                                                   *
 *    Modification date : $Date$
 *    Revision : $Revision$
 *    URL : $HeadURL$
 *             heyes@jlab.org                    Jefferson Lab, MS-12H        *
 *             Phone: (757) 269-7030             12000 Jefferson Ave.         *
 *             Fax:   (757) 269-5800             Newport News, VA 23606       *
 *                                                                            *
 *----------------------------------------------------------------------------*
 *
 * Description:
 *      Mutex protected FIFO
 *
 *----------------------------------------------------------------------------*/

#include <unistd.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>

#define _IN_FIFO_C

#include "emu_int_fifo.h"

circ_buf_t *
new_cb(char *name)
{
	circ_buf_t *cbp;

	cbp = (circ_buf_t *) malloc(sizeof (circ_buf_t));
	bzero((void *) cbp,sizeof(circ_buf_t));

	if (cbp == NULL)
		return (NULL);
	cbp->name = strdup(name);

	pthread_mutex_init(&cbp->buf_lock, NULL);
	pthread_cond_init(&cbp->notfull, NULL);
	pthread_cond_init(&cbp->notempty, NULL);
	pthread_cond_init(&cbp->empty, NULL);
	cbp->start_idx = 0;
	cbp->num_full = 0;
	return (cbp);
}

/*
 * put_cb_data() puts new data on the queue.
 * If the queue is full, it waits until there is room.
 */
int
put_cb_data(circ_buf_t *cbh, void *data)
{
  circ_buf_t *cbp = cbh;
  int st;

  if (cbh == NULL){
    return -1;
  }

  pthread_mutex_lock(&cbp->buf_lock);

  if (cbp->deleting) {
    pthread_mutex_unlock(&cbp->buf_lock);
    return -1;
  }

  cbp->wait_put = 1;

  /* wait while the buffer is full */
  while ((cbp->num_full == QSIZE) && (cbp->deleting == 0)) {
    /*     puts("EB data queue is full, waiting..."); */
    pthread_cond_wait(&cbp->notfull, &cbp->buf_lock);
  }

  cbp->wait_put = 0;

  if (cbp->deleting) {
    printf("fifo is being emptied!\n");
    pthread_cond_signal(&cbp->empty);
    pthread_mutex_unlock(&cbp->buf_lock);
    return -1;
  }

  cbp->data[(cbp->start_idx + cbp->num_full) % QSIZE] = data;
  cbp->num_full += 1;
  /* let a waiting reader know there's data */
  /* exercise: can cond_signal be moved after unlock? see text */
  pthread_cond_signal(&cbp->notempty);
  pthread_mutex_unlock(&cbp->buf_lock);
  return 0;
}


int
get_cb_count(circ_buf_t *cbh)
{
  circ_buf_t *cbp = cbh;
  int count;

  if (cbh == NULL) {
    return -1;
  }

  pthread_mutex_lock(&cbp->buf_lock);
  count = cbp->num_full;
  pthread_mutex_unlock(&cbp->buf_lock);
  return count;
}

/*
 * get_cb_data() gets the oldest data in the circular buffer.
 * If there is none, it waits until new data appears.
 */

void *
get_cb_data(circ_buf_t *cbh)
{
  circ_buf_t *cbp = cbh;
  void *data;

  if (cbh == NULL) {
    return ((void *) -1);
  }

  pthread_mutex_lock(&cbp->buf_lock);

  cbp->wait_get = 1;

  if (cbp->deleting && (cbp->num_full == 0)) {
    pthread_mutex_unlock(&cbp->buf_lock);
    return ((void *) -1);
  }
  /* wait while there's nothing in the buffer */
  while (cbp->num_full == 0) {
    /* printf("build thread: buffer empty, waiting for %s.\n", cbp->name); */
    pthread_cond_wait(&cbp->notempty, &cbp->buf_lock);
  }

  cbp->wait_get = 0;

  data = cbp->data[cbp->start_idx];
  cbp->start_idx = (cbp->start_idx + 1) % QSIZE;
  cbp->num_full -= 1;
  /* let a waiting writer know there's room */
  /* exercise: can cond_signal be moved after unlock? see text */
  pthread_cond_signal(&cbp->notfull);

  pthread_mutex_unlock(&cbp->buf_lock);

  return (data);
}

char
*get_cb_name(circ_buf_t *cbp)
{
  if (cbp == NULL) {
    return "UNKNOWN";
  } else {
    return cbp->name;
  }
}

/*
 * delete_cb() frees a circular buffer.
 */
void
delete_cb(circ_buf_t *cbh)
{
  circ_buf_t *cbp = cbh;

  if (cbh == NULL) return;

  pthread_mutex_trylock(&cbp->buf_lock);

  if (cbp->deleting) {
    pthread_mutex_unlock(&cbp->buf_lock);
    return;
  }

  cbp->deleting = 1;

  cbp->num_full = -1;

  pthread_cond_broadcast(&cbp->notempty);
  pthread_cond_broadcast(&cbp->notfull);

  pthread_mutex_unlock(&cbp->buf_lock);

  pthread_mutex_destroy(&cbp->buf_lock);
  pthread_cond_destroy(&cbp->notempty);
  pthread_cond_destroy(&cbp->empty);
  pthread_cond_destroy(&cbp->notfull);
  free(cbp->name);
  free(cbp);
}

/* EndSourceFile */
