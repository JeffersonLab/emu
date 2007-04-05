/*----------------------------------------------------------------------------
 *  Copyright (c) 2007        Southeastern Universities Research Association,
 *                            Thomas Jefferson National Accelerator Facility
 *
 *    This software was developed under a United States Government license
 *    described in the NOTICE file included as part of this distribution.
 *
 *    Author:  heyes
 *    Created: 9 Mar 2007
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
 *      emu  - emu_thread_package.h
 *
 *----------------------------------------------------------------------------*/
#ifndef EMU_THREAD_PACKAGE_H_
#define EMU_THREAD_PACKAGE_H_

#define EMU_THREAD_ENDED  0
#define EMU_THREAD_STARTING 1
#define EMU_THREAD_ACTIVE 2


struct emu_thread {
	char *name;
	int status;
	void *args;
	pthread_attr_t  attr;
	pthread_t       thread_id;
	struct emu_thread *prev;
	struct emu_thread *next;
};


#endif /*EMU_THREAD_PACKAGE_H_*/
