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
 *      emu  - emu_send_thread.h
 *
 *----------------------------------------------------------------------------*/
#ifndef EMU_SEND_THREAD_H_
#define EMU_SEND_THREAD_H_

#define FIFO_TYPE 1
#define ET_TYPE   2
 #include "et.h"

typedef  struct emu_stargs *emu_sender_id;

typedef struct emu_stargs {

    int type;
    int keep_going;
    char *target;
    et_sys_id the_et_id;
    et_att_id input_et_att;
    et_stat_id input_et_station;
    et_att_id output_et_att;
    et_stat_id output_et_station;
    struct cbt *input_fifo;

} emu_send_thread_args;

extern void emu_create_send_thread(emu_sender_id sender_id);

extern emu_sender_id emu_initialize_sender (int type, char *myname, char *target);

extern void *emu_FIFO_send_thread(void *arg);
extern void *emu_FIFO_test_thread(void *arg);
extern void *emu_ET_send_thread(void *arg);

#endif /*EMU_SEND_THREAD_H_*/
