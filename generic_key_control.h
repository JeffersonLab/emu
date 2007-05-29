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

void GKB_add_key(char key, int (*handler)(),void *arg, char *help);
int GKB_print_help();
void GKB_handler(void *arg);
int GKB_simple_quit();
#endif /*GENERIC_KEY_CONTROL_H_*/
