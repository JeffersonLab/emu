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
 *      Initialise threading for EMU.
 *
 *----------------------------------------------------------------------------*/

 #include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <sys/types.h>
#include <sys/mman.h>
#include <signal.h>
#include <unistd.h>
#include <time.h>
#include <dlfcn.h>
#ifdef sun
#include <thread.h>
#endif

#include "emu_sender.h"

void emu_init_thread_system(){
#ifdef sun
  int		  con, con_add;

  /*
   * want one thread for each station's conductor + extra threads
   * for heartbeat production and detection + 1 for main thd +
   * 1 for add stations thd + 2 for udp & tcp threads. However,
   * if we exceed system resources, we'll need to try something
   * more conservative.
   */
  con = thr_getconcurrency();
  if (thr_setconcurrency(con + config->nstations + ET_EXTRA_THREADS + 4) != 0) {
    /* exceeded # of threads allowed so reduce and try again */
    if (thr_setconcurrency(config->nstations + ET_EXTRA_THREADS + 4) != 0) {
      /* exceeded # of threads allowed so try fixed number */
      if (thr_setconcurrency(20) != 0) {
        /* exceeded # of threads allowed so let system choose */
	thr_setconcurrency(0);
      }
    }
  }
  con_add = thr_getconcurrency() - con;
  if (con_add < 1) {
    con_add = 0;
  }
#endif


}
