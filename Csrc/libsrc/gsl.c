/*----------------------------------------------------------------------------
 *  Copyright (c) 2007        Southeastern Universities Research Association,
 *                            Thomas Jefferson National Accelerator Facility
 *
 *    This software was developed under a United States Government license
 *    described in the NOTICE file included as part of this distribution.
 *
 *    Author:  heyes
 *    Created: Apr 30, 2007
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
 *      emu  - emu_sleeper.c
 *
 *----------------------------------------------------------------------------*/
 #include "gsl.h"
#include <unistd.h>
#include <time.h>
#include <sys/time.h>
 void gsl_sleep(int howlong_sec)
 {
 	struct timespec wait_sec;
 	            wait_sec.tv_sec  = howlong_sec;
            wait_sec.tv_nsec = 0;
 	nanosleep(&wait_sec, NULL);
 }
 
 double gsl_time() 
 {
 	double time_secs;
 	struct timeval tp;
 	gettimeofday(&tp, NULL);
 	
 	time_secs = tp.tv_sec + (tp.tv_usec/1000000.0);
 	return time_secs;
 }
