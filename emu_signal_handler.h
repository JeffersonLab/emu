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
 *      emu  - emu_signal_handler.h
 *
 *----------------------------------------------------------------------------*/

 #ifndef EMU_SIGNAL_HANDLER_H_
#define EMU_SIGNAL_HANDLER_H_
#include <sys/types.h>
#include <signal.h>
#ifdef sun
#include <thread.h>
#endif
#include <pthread.h>

void esh_create();
void esh_start();
void esh_add(char *name,void *(*handler_routine)(void *), void *arg)

#endif /*EMU_SIGNAL_HANDLER_H_*/
