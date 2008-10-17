/*----------------------------------------------------------------------------
 *  Copyright (c) 2007        Southeastern Universities Research Association, 
 *                            Thomas Jefferson National Accelerator Facility  
 *                                                                            
 *    This software was developed under a United States Government license    
 *    described in the NOTICE file included as part of this distribution.     
 *                                                                            
 *    Author:  heyes
 *    Created: Sep 20, 2007                      
 *    Modification date : $Date$
 *    Revision : $Revision$
 *    URL : $HeadURL$
 *                          
 *             heyes@jlab.org                    Jefferson Lab, MS-12H        
 *             Phone: (757) 269-7030             12000 Jefferson Ave.         
 *             Fax:   (757) 269-5800             Newport News, VA 23606       
 *                                                                            
 *----------------------------------------------------------------------------
 *
 * Description:
 *      sandbox  - gmp.c 
 *
 *----------------------------------------------------------------------------*/

#include <stdlib.h>
#include <stdio.h>
#include <sys/time.h>
#include <string.h>
#include <strings.h>
#include "gmp.hxx"

gmp_struc *gmp_new(char *name) {
	gmp_struc *gmp = (gmp_struc *) malloc(sizeof(gmp_struc));
	bzero ((void *)gmp, sizeof(gmp_struc));
	gmp->name = strdup(name);
	gmp->listeners = 0;
	gmp->read = 0;
	gmp->data = NULL;
	pthread_mutex_init(&gmp->lock, NULL);
	pthread_cond_init(&gmp->tx, NULL);
	pthread_cond_init(&gmp->cts, NULL);
	return gmp;
}

void gmp_subscribe(void *ojb,gmp_struc *gmp) {
	pthread_mutex_lock(&gmp->lock);
	gmp->listeners++;
	pthread_mutex_unlock(&gmp->lock);
}

void gmp_unsubscribe(gmp_struc *gmp) {
	pthread_mutex_lock(&gmp->lock);
	gmp->listeners--;
	pthread_mutex_unlock(&gmp->lock);
}

int gmp_get_subscribers(gmp_struc *gmp) {
	return gmp->listeners;
}

void gmp_send(gmp_struc *gmp, void *data) {
	pthread_mutex_lock(&gmp->lock);

	while (gmp->data != NULL) {
		int status;
		struct timespec ts;
		struct timeval tv;
		gettimeofday(&tv, NULL);
		ts.tv_sec = tv.tv_sec + 1;
		ts.tv_nsec = tv.tv_usec*1000;

		//printf("build thread: buffer empty, waiting for %s.\n", cbp->name);
		status = pthread_cond_timedwait(&gmp->cts, &gmp->lock, &ts);
		//printf("build thread: wakeup %s. status = %d\n", cbp->name, status);
	}
	gmp->data = data;

	pthread_cond_broadcast(&gmp->tx);
	pthread_mutex_unlock(&gmp->lock);

}

void *gmp_recieve(gmp_struc *gmp) {
	void *mydata;
	pthread_mutex_lock(&gmp->lock);
	gmp->listeners++;
	while (gmp->data == NULL) {
		int status;
		struct timespec ts;
		struct timeval tv;
		gettimeofday(&tv, NULL);
		ts.tv_sec = tv.tv_sec + 1;
		ts.tv_nsec = tv.tv_usec*1000;

		//printf("build thread: buffer empty, waiting for %s.\n", cbp->name);
		status = pthread_cond_timedwait(&gmp->tx, &gmp->lock, &ts);
		//printf("build thread: wakeup %s. status = %d\n", cbp->name, status);
	}
	mydata = gmp->data;
	pthread_mutex_unlock(&gmp->lock);
	return mydata;
}

void gmp_done(gmp_struc *gmp) {
	pthread_mutex_lock(&gmp->lock);
	gmp->listeners--;
	if (gmp->listeners == 0) {
		gmp->data = NULL;
		pthread_cond_broadcast(&gmp->cts);
	} else {
		while (gmp->data != NULL) {
			int status;
			struct timespec ts;
			struct timeval tv;
			gettimeofday(&tv, NULL);
			ts.tv_sec = tv.tv_sec + 1;
			ts.tv_nsec = tv.tv_usec*1000;

			//printf("build thread: buffer empty, waiting for %s.\n", cbp->name);
			status = pthread_cond_timedwait(&gmp->cts, &gmp->lock, &ts);
			//printf("build thread: wakeup %s. status = %d\n", cbp->name, status);
		}
	}
	pthread_mutex_unlock(&gmp->lock);
}

