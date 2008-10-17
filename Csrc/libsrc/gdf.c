/*----------------------------------------------------------------------------*
 *  Copyright (c) 2007        Southeastern Universities Research Association, *
 *                            Thomas Jefferson National Accelerator Facility  *
 *                                                                            *
 *    This software was developed under a United States Government license    *
 *    described in the NOTICE file included as part of this distribution.     *
 *                                                                            *
 *    Author:  Graham Heyes                                                   *
 *    Modification date : $Date: 2007-05-29 10:36:02 -0400 (Tue, 29 May 2007) $
 *    Revision : $Revision: 6167 $
 *    URL : $HeadURL: file:///daqfs/source/svnroot/coda/3.0/emu/emu_int_fifo.c $
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

#define _GDF_H_
#include <sys/time.h>
#include "gdf.h"
#include "gll.h"

static gdf_struc *gdf_fifos = NULL;
static pthread_mutex_t gdf_sys_lock;
static int gdf_max_fifos = 0;

#define DASH  "---------------------------------"
#define FALSE 0
#define TRUE  1

void gdf_sys_init(int max_fifos) {
    int ix;
    if (gdf_fifos == NULL) {
        gdf_max_fifos = max_fifos;
        gdf_fifos = (gdf_struc *) malloc(max_fifos * sizeof(gdf_struc));
        bzero((void*)gdf_fifos,max_fifos * sizeof(gdf_struc));

        pthread_mutex_init(&gdf_sys_lock, NULL);

        for (ix=0;ix<max_fifos;ix++) {
            pthread_mutex_init(&gdf_fifos[ix].buf_lock, NULL);
            pthread_cond_init(&gdf_fifos[ix].notfull, NULL);
            pthread_cond_init(&gdf_fifos[ix].notempty, NULL);
            pthread_cond_init(&gdf_fifos[ix].empty, NULL);
        }
    }
}

int gdf_new(char *name) {
    gdf_struc *cbp = NULL;
    int ix;
    pthread_mutex_lock(&gdf_sys_lock);
    for (ix=0;ix<gdf_max_fifos;ix++) {
        if (!gdf_fifos[ix].active) {
            cbp = &gdf_fifos[ix];
            gdf_fifos[ix].active = TRUE;
            break;
        }
    }
    pthread_mutex_unlock(&gdf_sys_lock);

    if (cbp == NULL)
        return -1;

    cbp->data = (long **) malloc(QSIZE*sizeof(void*));
    bzero((long *) cbp->data,QSIZE*sizeof(void*));
    cbp->qsize = QSIZE;

    cbp->name = strdup(name);

    cbp->start_idx = 0;
    cbp->num_full = 0;

    return ix;
}

int gdf_new_sized(char *name,int qsize) {
    gdf_struc *cbp = NULL;
    int ix;
    pthread_mutex_lock(&gdf_sys_lock);
    for (ix=0;ix<gdf_max_fifos;ix++) {
        if (!gdf_fifos[ix].active) {
            cbp = &gdf_fifos[ix];
            gdf_fifos[ix].active = TRUE;
            break;
        }
    }
    pthread_mutex_unlock(&gdf_sys_lock);

    if (cbp == NULL)
        return -1;

    cbp->data = (long **) malloc(qsize*sizeof(void*));
    bzero((long *) cbp->data,qsize*sizeof(void*));
    cbp->qsize = qsize;

    cbp->name = strdup(name);

    cbp->start_idx = 0;
    cbp->num_full = 0;

    return ix;
}

/*
 * gdf_put() puts new data on the queue.
 * If the queue is full, it waits until there is room.
 */
int
gdf_put(int fifo, long *data) {
    gdf_struc *cbp = &gdf_fifos[fifo];
    if (cbp->active == FALSE)
        return -1;

    pthread_mutex_lock(&cbp->buf_lock);

    cbp->wait_put = 1;

    /* wait while the buffer is full */
    if (cbp->num_full == cbp->qsize) {
        int status, nsec_total;
        struct timespec abs_time;
#if defined linux || defined __APPLE__

        struct timeval now;
        gettimeofday(&now, NULL);
        nsec_total = 1000*(250000 + now.tv_usec);
#else

        struct timespec now;
        clock_gettime(CLOCK_REALTIME, &now);
        nsec_total = 250000000 + now.tv_nsec;
#endif

        if (nsec_total >= 1000000000) {
            abs_time.tv_nsec = nsec_total - 1000000000;
            abs_time.tv_sec  = now.tv_sec + 1;
        } else {
            abs_time.tv_nsec = nsec_total;
            abs_time.tv_sec  = now.tv_sec;
        }

        status = pthread_cond_timedwait(&cbp->notfull, &cbp->buf_lock,&abs_time);
        //printf("build thread: buffer empty, waiting for %s.\n", cbp->name);
        if (status != 0) {
            //printf("build thread: wakeup %s. status = %d\n", cbp->name, status);

            cbp->wait_put = 0; // No longer waiting
            pthread_mutex_unlock(&cbp->buf_lock); // Free lock
            return status;
        }
    }
    cbp->wait_put = 0;

    cbp->data[(cbp->start_idx + cbp->num_full) % cbp->qsize] = data;
    cbp->num_full += 1;
    /* let a waiting reader know there's data */
    pthread_cond_signal(&cbp->notempty);
    pthread_mutex_unlock(&cbp->buf_lock);
    return 0;
}

/*
 * get_cb_data() gets the oldest data in the circular buffer.
 * If there is none, it waits until new data appears.
 */

int
gdf_get(int fifo, long **data) {

    gdf_struc *cbp = &gdf_fifos[fifo];
    *data = NULL;

    pthread_mutex_lock(&cbp->buf_lock);
    //printf("get from fifo %d\n",fifo);
    if (cbp->active == FALSE) {
        //printf("fifo not active from fifo %d\n",fifo);
        pthread_mutex_unlock(&cbp->buf_lock);
        return -1;
    }
    /* wait while there's nothing in the buffer */
    if (cbp->num_full == 0) {
        int status, nsec_total;
        struct timespec abs_time;
#if defined linux || defined __APPLE__

        struct timeval now;
        gettimeofday(&now, NULL);
        nsec_total = 1000*(250000 + now.tv_usec);
#else

        struct timespec now;
        clock_gettime(CLOCK_REALTIME, &now);
        nsec_total = 250000000 + now.tv_nsec;
#endif

        if (nsec_total >= 1000000000) {
            abs_time.tv_nsec = nsec_total - 1000000000;
            abs_time.tv_sec  = now.tv_sec + 1;
        } else {
            abs_time.tv_nsec = nsec_total;
            abs_time.tv_sec  = now.tv_sec;
        }

        //printf("build thread: buffer empty, waiting for %s.\n", cbp->name);
        status = pthread_cond_timedwait(&cbp->notempty, &cbp->buf_lock,&abs_time);
        //printf("build thread: wakeup %s. status = %d\n", cbp->name, status);
        if (cbp->active == FALSE) {
            //printf("get not active from fifo %d\n",fifo);
            pthread_mutex_unlock(&cbp->buf_lock);
            return -1;
        }
        if (status != 0) {
            cbp->wait_get = 0;
            //printf("get timeout from fifo %d\n",fifo);
            pthread_mutex_unlock(&cbp->buf_lock);
            return status;
        }
    }
    if (cbp->num_full == -1)
        return -1;

    cbp->wait_get = 0;

    *data = cbp->data[cbp->start_idx];
    //printf ("Data from fifo was %p stored at %p\n", *data, data);
    cbp->start_idx = (cbp->start_idx + 1) % cbp->qsize;
    cbp->num_full -= 1;
    /* let a waiting writer know there's room */
    pthread_cond_signal(&cbp->notfull);
    //printf("get done\n");
    pthread_mutex_unlock(&cbp->buf_lock);

    return 0;
}

int
gdf_count(int fifo ) {
    gdf_struc *cbp = &gdf_fifos[fifo];
    if (cbp->active == FALSE)
        return -1;

    int count;

    pthread_mutex_lock(&cbp->buf_lock);
    count = cbp->num_full;
    pthread_mutex_unlock(&cbp->buf_lock);
    return count;
}

char
*gdf_name(int fifo) {
    gdf_struc *cbp = &gdf_fifos[fifo];
    if (cbp->active == FALSE)
        return NULL;

    else
        return cbp->name;

}

/*
 * delete_cb() frees a circular buffer.
 */
void
gdf_free(int fifo) {
    gdf_struc *cbp = &gdf_fifos[fifo];
    if (cbp->active == FALSE)
        return;

    pthread_mutex_lock(&cbp->buf_lock);
    printf("free fifo %d\n", fifo);
    cbp->num_full = 0;
    cbp->wait_get = 0;
    cbp->wait_put = 0;
    cbp->start_idx = 0;

    cbp->active = FALSE;

    pthread_cond_broadcast(&cbp->notempty);
    pthread_cond_broadcast(&cbp->notfull);

    free(cbp->data);
    free(cbp->name);
    printf("free done\n");
    pthread_mutex_unlock(&cbp->buf_lock);
}


int gdf_list(void *arg) {
    int ix;
    gdf_struc *cbp;
    printf ("%.26s\n",DASH);
    printf ("%-20s %5s\n", "List name","count");
    printf ("%-.20s %.5s\n",DASH,DASH);

    for (ix=0;ix<gdf_max_fifos;ix++) {
        cbp = &gdf_fifos[ix];
        ;
        if (cbp->active)
            printf ("%-20s %5d\n", gdf_name(ix),gdf_count(ix));
    }

    printf ("%.26s\n",DASH);

    return 0;
}

/* EndSourceFile */
