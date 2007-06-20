/*----------------------------------------------------------------------------
 *  Copyright (c) 2007        Southeastern Universities Research Association,
 *                            Thomas Jefferson National Accelerator Facility
 *
 *    This software was developed under a United States Government license
 *    described in the NOTICE file included as part of this distribution.
 *
 *    Author:  heyes
 *    Created: Mar 22, 2007
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
 *      emu  - emu_sender.c
 *
 * 	This file contains the code for the "sender" module of the EMU. The code has
 * been written to be reusable in the ROC. The module has two modes of operation.
 * FIFO mode - a mutex protected circular FIFO is created as a data source.
 * ET mode - a local ET system is created as the data source.
 *
 * Data records are added to the ET or FIFO by the data producer. That is a
 * readout list in the case of ROC and build thread in the case of the EMU.
 *
 * A thread called the Sender is created that waits for data to be availible on
 * the input and writes it to a remote ET system. To increase efficiency the
 * output ET events are requested in NOALLOC mode. This allows one copy to be
 * saved since we pass to the output ET a pointer to the existing record rather
 * than copying the data from the input record to the output record.
 *----------------------------------------------------------------------------*/
#include "emu_sender.h";
#include "emu_process.h"
#include "emu_record_format.h"

#define TRUE 1
#define FALSE 0

static unsigned long dummy_records[256][10000];

/*static void interrupt_signal_handler(void *arg)
{
    emu_sender_id sender_id = (emu_sender_id) arg;

    printf("Ignore CONTROL-C\n");
    //emu_sender_stop(sender_id);
}*/

static int emu_sender_simulate_pause(emu_sender_id sender_id) {
    sender_id->pause = TRUE;
}
static int emu_sender_simulate_go(emu_sender_id sender_id) {
    sender_id->pause = FALSE;
}
void *emu_et_monitor(void *arg) {
    struct gtp_thread *thread_descriptor = (struct gtp_thread *) arg;
    emu_sender_id sender_id = (emu_sender_id) thread_descriptor->args;
    int status = 0;
    et_sys_id	 id;
    et_openconfig  openconfig;
    struct timespec time_to_wait;

    /* This thread takes care of the connection to ET. We are a process remote from the ET so
     * everything happens over  the network. It's done as a thread because I don't want the open
     * to block the whole program.
    */

    et_open_config_init(&openconfig);

    et_open_config_setmode(openconfig, ET_HOST_AS_REMOTE);
    et_open_config_sethost(openconfig,gph_get_param("/component/send/output/host"));
	et_open_config_setport(openconfig,atoi("/component/send/output/port"));
    et_open_config_setwait(openconfig,ET_OPEN_WAIT);
    et_open_config_settimeout(openconfig,time_to_wait);
    et_open_config_setpolicy(openconfig,ET_POLICY_FIRST);

    printf("\n\nHERE LOOKING FOR %s\n", sender_id->target);

    if (et_open(&id, sender_id->target, openconfig) != ET_OK) {
        printf("et_netclient: cannot open ET system\n");
        exit(1);
    }

    printf("DONE\n\n\n");
    et_open_config_destroy(openconfig);

    /* This could be confusing so here's a comment...
     * The output of this component is attached to the input of the next component in the
     * data-stream. So we find the input station and attach to it but the station id and attachment
     * are stored in the fields called output NOT the ones called input...
    */

    char station_name[100];
    sprintf(station_name,"%s_input",gph_get_param("/component/name"));
    if (et_station_name_to_id(id,&sender_id->output_et_station,station_name) < 0) {
        EMU_DEBUG(("error finding station %s",station_name));
        exit(1);
    }

    if (et_station_attach(id, sender_id->output_et_station, &sender_id->output_et_att) < 0) {
        printf("et_netclient: error in station attach\n");
        exit(1);
    }

    sender_id->the_et_id = id;
    switch (sender_id->type) {
    case FIFO_TYPE:
        EMU_DEBUG(("input is from a FIFO"));
        sender_id->sender = gtp_create(1,"FIFO send thread 0", emu_sender_process, (void *) sender_id);
        sender_id->getter = gtp_create(1,"ET buffer thread 0", emu_sender_etmtfifo, (void *) sender_id);
        sender_id->pause = TRUE;
        if (gph_get_value("/component/proc/ID") == NULL) {
            gkb_add_key('g', emu_sender_simulate_go, (void *) sender_id,"start data taking (go)");
            gkb_add_key('p', emu_sender_simulate_pause, (void *) sender_id, "pause data taking");
            sender_id->tester = gtp_create(1,"FIFO test thread 0", emu_sender_simulate, (void *) sender_id);
        }
        break;
    case ET_TYPE:
    default:
        EMU_DEBUG(("default input is from a ET"));
        //gtp_create(1,"ET send thread 0", emu_ET_send_thread, (void *) sender_id);
        break;
    }
    gtp_cancel(thread_descriptor);
}

/*****************************************************
 *
 * emu emu_sender.c
 * Function emu_initialize_sender
 *
 * Arguments:
 * 	type of input data source.
 *  name of this CODA component.
 * 	name of output ET system.
 * The sender has two modes, FIFO or ET input data source. This function
 * initializes either a FIFO or a local ET system as the input data source
 * and attaches to a remote ET system as the output data sink.
 *
 */

emu_sender_id emu_sender_initialize () {
    int status;
    emu_sender_id sender_id;
    gdf_struc *fifo;
	char *target_name = gph_get_param("/component/send/output/name");
    if ((target_name == NULL) || (strcmp(target_name,"") == 0)) {
        return NULL;
    }

    sender_id = (emu_sender_id) malloc(sizeof(emu_send_thread_args));
    bzero(sender_id, sizeof(emu_send_thread_args));

    sender_id->type = FIFO_TYPE;


    sender_id->port = atoi(gph_get_param("/component/send/output/port"));
    sender_id->target = gph_get_param("/component/send/output/name");


    printf ("target = %s port= %d\n",sender_id->target,sender_id->port);

    if (gph_get_value("/component/proc/ID") != NULL) {
        emu_process_id id = gph_get_value("/component/proc/ID");
        fifo = id->output;
    } else {
        fifo = gdf_new("EMU input FIFO");
    }
    sender_id->input_fifo = fifo;
    sender_id->etmt_fifo = gdf_new_sized("ET new event fifo",EMU_SENDER_QSIZE);

    gsh_block();
    //gsh_add("sender Control-C handler", interrupt_signal_handler, (void *) sender_id);

    return sender_id;
error:
    free(sender_id);
    return NULL;
}


/*
 * emu emu_sender.c
 * Function emu_create_send_thread
 *
 * This function creates the thread that does all the work.
 * For debugging purposes it can also create a test thread for FIFO mode
 * that loops poking dummy records into the input FIFO.
 */


void emu_sender_start(emu_sender_id sender_id) {

    int status;

    EMU_DEBUG (("Create send thread with sender_id %08x type %d", sender_id,sender_id->type));

    // The send thread will run forever unless there is an error or the keep_going
    // flag is cleared.

    sender_id->keep_going = 1;
    /*
     * Both types of sender send their data to an ET system.
     */

    sender_id->sender = gtp_create(1,"ET connector thread 0", emu_et_monitor, (void *) sender_id);

    return;
}

void emu_sender_stop(emu_sender_id sender_id) {
    gdf_struc *fifo;
    printf("Stop Sender ET\n",sender_id);

    gtp_cancel(sender_id->getter);
    gtp_cancel(sender_id->sender);

    if (gph_get_value("/component/proc/ID") == NULL) {
        fifo = sender_id->input_fifo;
        sender_id->input_fifo =NULL;
        if (fifo != NULL)
            gdf_delete(fifo);

        fifo = sender_id->etmt_fifo;
        sender_id->etmt_fifo =NULL;

        if (fifo != NULL)
            gdf_delete(fifo);
    }
    sender_id->keep_going = 0;
    printf("Force ET close\n");
    et_forcedclose(sender_id->the_et_id);
    printf("sender stopped\n");
}

void *emu_sender_etmtfifo(void *arg) {

    struct gtp_thread *thread_descriptor = (struct gtp_thread *) arg;
    int status = 0;
    et_event   *pe[1000];
    int size, actual;
    emu_sender_id sender_id = (emu_sender_id) thread_descriptor->args;
    et_sys_id et_id;
    et_att_id att;
    int ix, count;
    struct timespec doze;
    gtp_enable_cancel();

    while (sender_id->keep_going) {
        et_id = sender_id->the_et_id;
        att = sender_id->output_et_att;
        if (et_id == NULL) {
            printf("etmtfifo waiting for ET\n");
            gsl_sleep(1);
            continue;
        }

        if (et_alive(et_id) == 0) {
            EMU_DEBUG(("et_alive returned FALSE\n"))
            break;
        }
        /* Ask the ET system for enough ET events to hold the number of records in the queue
          * plus the one we just pulled off the top. ET returns an array of ET events that may
          * be less than the number we asked for but holds at least one event.
          */
        count = EMU_SENDER_QSIZE;
        // - get_cb_count(sender_id->etmt_fifo);

        doze.tv_nsec = 0;
        doze.tv_sec = 1;
        status = et_events_get(et_id,att,pe, ET_TIMED | ET_NOALLOC | ET_MODIFY, &doze,count,&actual);

        //printf("count = %d actual = %d\n",count,actual);

        if (status == ET_ERROR_TIMEOUT)
            continue;
        if (status != ET_OK) {
            printf("error in et_event_new\n");

            break;
        }

        for (ix = 0; ix < actual;ix++)
            put_cb_data(sender_id->etmt_fifo, (void *) pe[ix]);
    }
    sender_id->keep_going = FALSE;
    gtp_cancel(thread_descriptor);
}

/*
 * author heyes
 *
 * The thread to do all of the work.
 */

void *emu_sender_process(void *arg) {
    struct gtp_thread *thread_descriptor = (struct gtp_thread *) arg;
    int status = 0;
    et_event   *pe[1000];
    int counter, ninput, nfree_et;
    emu_sender_id sender_id = (emu_sender_id) thread_descriptor->args;
    emu_process_id process_id;
    et_sys_id et_id;
    et_att_id att;

    process_id = gph_get_value("/component/proc/ID");
    printf("I am a thread named \"%s\" my sender_id is %08x input type is %d\n", thread_descriptor->name, sender_id, sender_id->type);

    gtp_enable_cancel();

    counter = 0;

    while ( sender_id->keep_going) {
        if ( (counter == EMU_SENDER_QSIZE) || ((counter > 0) && ((gdf_count(sender_id->etmt_fifo) == 0) || (gdf_count(sender_id->input_fifo) == 0)))) {
            et_id = sender_id->the_et_id;
            att = sender_id->output_et_att;
            if (et_id == NULL) {
                printf("sender waiting for ET\n");
                gsl_sleep(1);
                continue;
            }

            //printf("sending with counter %d\n",counter);
            status = et_events_put(et_id,att, pe,counter);
            counter = 0;
            if (status != ET_OK) {
                printf("et_producer: put error\n");
                // instead of breaking do something smart here!!
                et_id = NULL;
                continue;
            }
        }

        emu_data_record_ptr record = (emu_data_record_ptr)  gdf_get(sender_id->input_fifo);
        if (((int) record == -1)||(record == NULL))
            break;

        pe[counter] = (et_event *) gdf_get(sender_id->etmt_fifo);
        if (((int) pe[counter] == -1) || (pe[counter] == NULL))
            break;


        pe[counter]->pdata = record;
        pe[counter]->length = record->record_header.length;
        pe[counter]->control[0] = sender_id->output_et_station+1;
        if (process_id == NULL)
            pe[counter]->owner = ET_NOALLOC;

        counter ++;
    }

    gtp_cancel(thread_descriptor);
    sender_id->keep_going = 0;
    return;
}


/*
 * emu emu_sender.c
 * Function emu_FIFO_test_thread
 *
 * This code pours simulated records into the input fifo as quickly as possible.
 * The simulated data is stored in a statically allocated array to remove any delays due
 * to calling malloc.
 */

void *emu_sender_simulate(void *arg) {
    struct gtp_thread *thread_descriptor = (struct gtp_thread *) arg;
    int counter, status = 0;
    int recordCounter = 0;
    gdf_struc *fifo;

    emu_sender_id sender_id = (emu_sender_id) thread_descriptor->args;
    fifo = sender_id->input_fifo;

    printf("I am a thread named \"%s\" my sender_id is %08x input type is %d\n", thread_descriptor->name, sender_id, sender_id->type);
    while( sender_id->keep_going && ( sender_id->input_fifo != NULL) && (status == 0)) {
        if (sender_id->pause) {
            gsl_sleep(1);
            continue;
        }
        // Convert data to a record
        emu_data_record_ptr record = (emu_data_record_ptr) &dummy_records[recordCounter & 0xFF][0];
        record->record_header.length = 5000;
        record->record_header.recordNB = recordCounter;

        recordCounter++;

        record->record_header.rocID = sender_id->output_et_station;

        record->record_header.payload[0] = 0x12345678;
        record->record_header.payload[1] = 0xDEADD00D;
        record->record_header.payload[2] = 0xC0DA4DA0;
        record->record_header.payload[3] = 0xCA117030;
        record->record_header.payload[4] = 0xCEBAF000;
        record->record_header.payload[5] = 0xBEEF3142;

        if (recordCounter % 10000 == 0)
            printf("Another 10k records, %d so far\n",recordCounter);

        //EMU_DEBUG(("PUT Data %08x, len = %d",&dummy_records[0][0],dummy_records[0][0] ))

        put_cb_data(fifo,(void *) record);
    }

clean_exit_emu_send_thread:
    gtp_cancel(thread_descriptor);
    return;
}



