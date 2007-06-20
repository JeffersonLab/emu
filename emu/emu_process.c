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
 *      emu  - emu_process.c
 *
 *----------------------------------------------------------------------------*/
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include "support/gph.h"
#include "support/gsl.h"
#include "emu_process.h"
#include "emu_reader.h"
#include "emu_sender.h"

static void interrupt_signal_handler(void *arg) {

    emu_process_id process_id = (emu_process_id) arg;

    printf("Process thread Interrupted by CONTROL-C\n");

    emu_process_stop(process_id);

}

emu_process_id emu_process_initialize() {
    emu_process_id self;
    emu_reader_id reader_id;
    // we only need to process if we have a reader

    if (gph_get_value("/component/read/ID") == NULL)
        return NULL;

    // allocate storage and create FIFOs.
    self = malloc(sizeof(emu_process_desc));

    bzero (self,sizeof(emu_process_desc));

    // output fifo
    self->output = gdf_new("process output");
    gph_add_value("process output",self->output);

    //printf ("self %08x in %08x out %08x\n",self, self->input,self->output);

    // output will be linked by sender when it is created.

    gsh_add("process Control-C handler", interrupt_signal_handler, (void *) self);

    return self;
error:
    free(self);
    return NULL;
}

void emu_process_process(void *arg) {
    struct gtp_thread *thread_descriptor = (struct gtp_thread *) arg;
    emu_process_id process_id = (emu_process_id) thread_descriptor->args;
    emu_reader_id reader_id = gph_get_value("/component/read/ID");
    emu_sender_id sender_id = gph_get_value("/component/send/ID");

    //printf ("process_id  %08x in %08x out %08x\n",process_id, process_id->input,process_id->output);
    while (process_id->input == NULL) {
        void *in = gph_get_value("reader output");

        if (in != NULL)
            process_id->input = in;
        else
            gsl_sleep(1);
    }
    while(1) {
        char *data_in, *data_out;
        et_event   *pe;
        emu_data_record_ptr record;
        int ix, status;
        et_sys_id id = reader_id->id;
        et_id      *etid = (et_id *) id;
        //printf("process_process waiting for data\n");
        /* Get a record right at the start. We do this so that we block
         * here if there is no work to do.
         */
        // for debug gsl_sleep(20);
        if ((process_id->input == NULL) || (process_id->output == NULL))
            break;

        printf ("call get on %08x\n",process_id->input);
        data_in = gdf_get(process_id->input);
        printf("done\n");

        if ((int) data_in == -1) {
            printf("a %08x\n",data_in);
            break;
        }
        pe = (et_event *)data_in;
        record = (emu_data_record_ptr) pe->pdata;

        if (sender_id != NULL)
        {
            data_out = (char *) malloc(pe->length);
            bcopy((void *) record, (void *) data_out,pe->length);
            if ((process_id->input == NULL) || (process_id->output == NULL)) {
                free(data_out);
                printf("b\n");

            } else
            if (put_cb_data(process_id->output,data_out) == -1) {
                printf("c\n");
                break;
            }
        }/* else {
                     if (0) {
                        printf ("process_process- record number %d from input %d\n",
                                record->record_header.recordNB,
                                record->record_header.rocID);

                        printf ("   length is %d\n", record->record_data.length);
                        for (ix=0;ix<10;ix++) {
                            printf("     data[%2d] - %08X\n",ix, record->record_data.data[ix]);
                        }
                        printf ("---------------------\n\n");
                    }
                }*/
        pe->length = 0;
        printf("et_event_put\n");
        status = et_event_put(etid,reader_id->gc_att, pe);
        printf("et_event_put done\n");
        if (status != ET_OK) {
            EMU_DEBUG(("put error"));
            thread_descriptor->status = _THREAD_ENDED;
            return;
        }
    }
    gtp_cancel(thread_descriptor);
}

void emu_process_stop(emu_process_id process_id) {
    gdf_struc *tmp;
    printf("process stop called\n");
    //tmp = process_id->input;
    //process_id->input = NULL;
    //gdf_delete(tmp);
    tmp = process_id->output;
    process_id->output = NULL;
    gdf_delete(tmp);

}

void emu_process_start(emu_process_id process_id) {
    char thread_name[200];

    sprintf(thread_name, "process thread %d",1);

    process_id->process = gtp_create(1,thread_name, emu_process_process, (void *) process_id);
}

void emu_process_simulate(emu_process_id process_id) {}


