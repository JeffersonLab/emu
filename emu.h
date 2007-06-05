/*----------------------------------------------------------------------------
 *  Copyright (c) 2007        Southeastern Universities Research Association,
 *                            Thomas Jefferson National Accelerator Facility
 *
 *    This software was developed under a United States Government license
 *    described in the NOTICE file included as part of this distribution.
 *
 *    Author:  heyes
 *    Created: Apr 23, 2007
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
 *      emu  - emu_configuration.h
 *
 *----------------------------------------------------------------------------*/


#ifndef EMU_CONFIGURATION_H_
#define EMU_CONFIGURATION_H_

#include "emu_reader.h"
#include "emu_process.h"
#include "emu_sender.h"


#define EMU_MODE_NORMAL  0
#define EMU_MODE_SIMULATE 1

int emu_initialize(int argc,char **argv);

typedef struct emu_config_struct_t *emu_config_ptr;

typedef struct emu_config_struct_t  {
	char *emu_name;
	char *output_target_name;
	char *output_target_host;
	char *input_names[EMU_MAX_INPUTS];
	int port;
	int input_count;
	int reader_mode;
	int process_mode;
	int sender_mode;
	emu_reader_id read;
	emu_process_id process;
	emu_sender_id send;
} emu_config_t;

extern emu_config_ptr emu_config ();
#ifndef EMU_MAIN
extern emu_config_ptr emu_configuration;
#endif
#endif /*EMU_CONFIGURATION_H_*/
