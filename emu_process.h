/*----------------------------------------------------------------------------
 *  Copyright (c) 2007        Southeastern Universities Research Association,
 *                            Thomas Jefferson National Accelerator Facility
 *
 *    This software was developed under a United States Government license
 *    described in the NOTICE file included as part of this distribution.
 *
 *    Author:  heyes
 *    Created: May 2, 2007
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
 *      emu  - emu_process.h
 *
 *----------------------------------------------------------------------------*/

#ifndef EMU_PROCESS_H_
#define EMU_PROCESS_H_

#include "emu_int_fifo.h"

typedef  struct emu_process *emu_process_id;

typedef struct emu_process
{
    circ_buf_t *input;
    circ_buf_t *output;
    struct emu_thread *process;
}
emu_process_desc;

emu_process_id emu_process_initialize ( );
void emu_process_start(emu_process_id reader_id);
void emu_process_stop(emu_process_id reader_id);
#endif /*EMU_PROCESS_H_*/
