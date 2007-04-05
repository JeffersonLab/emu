/*----------------------------------------------------------------------------
 *  Copyright (c) 2007        Southeastern Universities Research Association,
 *                            Thomas Jefferson National Accelerator Facility
 *
 *    This software was developed under a United States Government license
 *    described in the NOTICE file included as part of this distribution.
 *
 *    Author:  heyes
 *    Created: 7 Mar 2007
 *
 *             heyes@jlab.org                    Jefferson Lab, MS-12H
 *             Phone: (757) 269-7030             12000 Jefferson Ave.
 *             Fax:   (757) 269-5800             Newport News, VA 23606
 *
 *----------------------------------------------------------------------------
 *
 * Description:
 *      emu  - emu_reader.h
 *
 *----------------------------------------------------------------------------*/
#ifndef EMU_READER_H_
#define EMU_READER_H_

#include "et_private.h"
#include "et_network.h"
#include "emu_common.h"
#include "emu_thread_package.h"
#include "emu_int_data_struct.h"

#define EMU_MAX_INPUTS 100
#define EMU_READER_QUEUE_SIZE 20

typedef struct emu_input *emu_input_id;

typedef struct emu_input {
	et_stat_id	 input_station;
	et_stat_id	 output_station;
	et_att_id     input_att;
	et_att_id     output_att;

} emu_input_desc;

typedef  struct emu_reader *emu_reader_id;

typedef struct emu_reader
{
    et_sys_id     id;
    int keep_going;
    et_sys_id output_et_id;
	et_att_id     gc_att;
    int number_inputs;
	emu_input_desc inputs[EMU_MAX_INPUTS];
}
emu_reader_desc;

#endif /*EMU_READER_H_*/
