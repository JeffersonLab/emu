/*----------------------------------------------------------------------------
 *  Copyright (c) 2007        Southeastern Universities Research Association,
 *                            Thomas Jefferson National Accelerator Facility
 *
 *    This software was developed under a United States Government license
 *    described in the NOTICE file included as part of this distribution.
 *
 *    Author:  heyes
 *    Created: 9 Mar 2007
 *    Modification date : $Date: 2007-06-05 10:03:31 -0400 (Tue, 05 Jun 2007) $
 *    Revision : $Revision: 6174 $
 *    URL : $HeadURL: file:///daqfs/source/svnroot/coda/3.0/emu/emu_thread_package.h $
 *
 *             heyes@jlab.org                    Jefferson Lab, MS-12H
 *             Phone: (757) 269-7030             12000 Jefferson Ave.
 *             Fax:   (757) 269-5800             Newport News, VA 23606
 *
 *----------------------------------------------------------------------------
 *
 * Description:
 *      emu  - emu_thread_package.h
 *
 *----------------------------------------------------------------------------*/
#ifndef _GTP_H_
#define _GTP_H_

#define _THREAD_ENDED  0
#define _THREAD_STARTING 1
#define _THREAD_ACTIVE 2
#define MONITOR_PERIOD 1 // one second

#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <sys/types.h>
#include <signal.h>
#ifdef sun
#include <thread.h>
#endif
#include <pthread.h>

struct gtp_thread {
	char *name;
	int status;
	void *args;
	pthread_attr_t  attr;
	pthread_t       thread_id;
	struct gtp_thread *prev;
	struct gtp_thread *next;
};

struct gtp_thread *gtp_create(int detatched,char *name, void *thread_body, void *thread_args);
void gtp_cancel(struct gtp_thread *thread_descriptor);
int gtp_list();

#endif /*_GTP_H_*/
