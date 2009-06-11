/*----------------------------------------------------------------------------
 *  Copyright (c) 2007        Southeastern Universities Research Association,
 *                            Thomas Jefferson National Accelerator Facility
 *
 *    This software was developed under a United States Government license
 *    described in the NOTICE file included as part of this distribution.
 *
 *    Author:  heyes
 *    Created: Apr 17, 2007
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
 *      emu  - generic_key_control.h
 *
 *----------------------------------------------------------------------------*/

#ifndef GENERIC_KEY_CONTROL_H_
#define GENERIC_KEY_CONTROL_H_

#include "gmp.hxx"

#ifdef __cplusplus
extern "C" void gkb_add_key(char key, int (*handler)(void* arg),void *arg, char *help);
extern "C" int gkb_print_help(void *arg);
extern "C" void gkb_handler(void *arg);
extern "C" int gkb_simple_quit(void *arg);
extern "C" void gkb_start();
extern "C" void gkb_stop();
#else
extern void gkb_add_key(char key, int (*handler)(void* arg),void *arg, char *help);
extern int gkb_print_help(void *arg);
extern void gkb_handler(void *arg);
extern int gkb_simple_quit(void *arg);
extern void gkb_start();
extern void gkb_stop();
#endif
#endif /*GENERIC_KEY_CONTROL_H_*/
