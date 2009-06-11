/*----------------------------------------------------------------------------
 *  Copyright (c) 2007        Southeastern Universities Research Association,
 *                            Thomas Jefferson National Accelerator Facility
 *
 *    This software was developed under a United States Government license
 *    described in the NOTICE file included as part of this distribution.
 *
 *    Author:  heyes
 *    Created: 7 Mar 2007
 *    Modification date : $Date: 2007-06-05 10:03:31 -0400 (Tue, 05 Jun 2007) $
 *    Revision : $Revision: 6174 $
 *    URL : $HeadURL: file:///daqfs/source/svnroot/coda/3.0/emu/emu_int_fifo.h $
 *
 *             heyes@jlab.org                    Jefferson Lab, MS-12H
 *             Phone: (757) 269-7030             12000 Jefferson Ave.
 *             Fax:   (757) 269-5800             Newport News, VA 23606
 *
 *----------------------------------------------------------------------------
 *
 * Description:
 *      emu  - emu_int_fifo.h
 *
 *----------------------------------------------------------------------------*/

#ifndef _GMP_
#define _GMP_
#include <pthread.h>

typedef struct gmp {
	char *name;

	int listeners;
	int read;

	void *data;
	pthread_mutex_t lock;
	pthread_cond_t tx;
	pthread_cond_t cts;
} gmp_struc;

#ifdef __cplusplus
extern "C" gmp_struc *gmp_new(char *name);
extern "C" void gmp_subscribe(void *ojb,gmp_struc *gmp);
extern "C" void gmp_send(gmp_struc *gmp, void *data);
extern "C" void *gmp_recieve(gmp_struc *gmp);
extern "C" void gmp_done(gmp_struc *gmp);
#else
gmp_struc *gmp_new(char *name);
void gmp_subscribe(gmp_struc *gmp);
void gmp_send(gmp_struc *gmp, void *data);
void *gmp_recieve(gmp_struc *gmp);
void gmp_done(gmp_struc *gmp) ;
#endif
#endif /*_GMP_*/

