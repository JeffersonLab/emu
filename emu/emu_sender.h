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
#define EMU_SENDER_QSIZE 60
 #include "et.h"
 #include "emu_utilities.h"
 #include "support/gtp.h"
 #include "support/gdf.h"
 #include "support/gph.h"

typedef  struct emu_stargs *emu_sender_id;

typedef struct emu_stargs {

    int type;
    int keep_going;
    int pause;
    char *target;
    int port;
    et_sys_id the_et_id;
    et_att_id input_et_att;
    et_stat_id input_et_station;
    et_att_id output_et_att;
    et_stat_id output_et_station;
    struct gdf *input_fifo;
    struct gdf *etmt_fifo;
    struct gtp_thread *attacher;
    struct gtp_thread *sender;
    struct gtp_thread *getter;
    struct gtp_thread *tester;

} emu_send_thread_args;

extern emu_sender_id emu_sender_initialize ();
extern void emu_sender_start(emu_sender_id sender_id);
extern void emu_sender_stop(emu_sender_id sender_id);
extern void *emu_sender_process(void *arg);
extern void *emu_sender_simulate(void *arg);
extern void *emu_sender_etmtfifo(void *arg);
#endif /*EMU_SEND_THREAD_H_*/
