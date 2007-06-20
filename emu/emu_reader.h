/*----------------------------------------------------------------------------
 *  Copyright (c) 2007        Southeastern Universities Research Association,
 *                            Thomas Jefferson National Accelerator Facility
 *
 *    This software was developed under a United States Government license
 *    described in the NOTICE file included as part of this distribution.
 *
 *    Author:  heyes
 *    Created: 7 Mar 2007
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
 *      emu  - emu_reader.h
 *
 *----------------------------------------------------------------------------*/
#ifndef EMU_READER_H_
#define EMU_READER_H_

#include "et_private.h"
#include "et_network.h"
#include "emu_utilities.h"
#include "emu_record_format.h"
#include "support/gtp.h"
#include "support/gdf.h"

#define EMU_MAX_INPUTS 100
#define EMU_READER_QUEUE_SIZE 200

#define TIME_BEFORE_REMOVE 1

#define EMU_MODE_PROCESS 0
#define EMU_MODE_SIMULATE 1

typedef struct emu_input *emu_input_id;

typedef struct emu_input
{
    et_stat_id	 input_station;
    et_stat_id	 output_station;
    et_att_id     input_att;
    et_att_id     output_att;

    long long record_count;
    long long word_count;

}
emu_input_desc;

typedef  struct emu_reader *emu_reader_id;

typedef struct emu_reader
{
	int mode;
    et_sys_id     id;
    int keep_going;
    gdf_struc *reader_output;
    et_att_id     gc_att;
    int number_inputs;
    emu_input_desc inputs[EMU_MAX_INPUTS];

    long long record_count;
    long long word_count;

    struct gtp_thread *worker_thread;
}
emu_reader_desc;

emu_reader_id emu_reader_initialize ( );
void emu_reader_start(emu_reader_id reader_id);
void emu_reader_stop(emu_reader_id reader_id);

#endif /*EMU_READER_H_*/
