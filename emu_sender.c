/*----------------------------------------------------------------------------
 *  Copyright (c) 2007        Southeastern Universities Research Association,
 *                            Thomas Jefferson National Accelerator Facility
 *
 *    This software was developed under a United States Government license
 *    described in the NOTICE file included as part of this distribution.
 *
 *    Author:  heyes
 *    Created: Mar 22, 2007
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
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <sys/types.h>
#include <sys/mman.h>
#include <signal.h>
#include <unistd.h>
#include <time.h>
#include <dlfcn.h>
#ifdef sun
#include <thread.h>
#endif
#include <pthread.h>

#include "et.h"

#include "emu_common.h"
#include "emu_sender.h"
#include "emu_thread_package.h"
#include "emu_int_fifo.h"
#include "emu_int_data_struct.h"

static void *emu_send_thread(void *arg);

static unsigned long dummy_records[256][1000];

/*****************************************************
 *
 * emu emu_send_thread.c
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

emu_sender_id emu_initialize_sender (int type, char *myname, char *target)
{
    int status;
    emu_sender_id sender_id = (emu_sender_id) malloc(sizeof(emu_send_thread_args));
    bzero(sender_id, sizeof(emu_send_thread_args));

    sender_id->type = type;
    sender_id->target = strdup(target);

    switch (type)
    {
    case FIFO_TYPE:
        {
            circ_buf_t *fifo;
            EMU_DEBUG(("input is from a FIFO sender_id %08x",sender_id));
            fifo = new_cb("EMU input FIFO");

            sender_id->input_fifo = fifo;

            break;
        }
    case ET_TYPE:
    default:
        EMU_DEBUG(("default input is from a ET called %s", sender_id->target));
        break;
    }

    /*
     * Both types of sender send their data to an ET system.
     */
    {
        et_sys_id	 id;
        et_stat_id	 my_stat;
        et_att_id	 my_att;
        et_openconfig  openconfig;
        int selections[] = {0,-1,-1,-1};
        /*
         * We Initialize the output ET system and tell it that it is just
         * a "front" for a remote system.
         */

        et_open_config_init(&openconfig);
        et_open_config_setmode(openconfig, ET_HOST_AS_REMOTE);

        et_open_config_setcast(openconfig, ET_BROADCAST);
        //et_open_config_addmulticast(openconfig, ET_MULTICAST_ADDR);
        //et_open_config_setTTL(openconfig, 2);


        /* For direct access to a remote ET system, use ET_DIRECT
         * and specify the port that the server is on.
         */

        //et_open_config_setcast(openconfig, ET_DIRECT);
        et_open_config_sethost(openconfig, "albanac.jlab.org");
        et_open_config_setserverport(openconfig, 11111);


        if (et_open(&id, sender_id->target, openconfig) != ET_OK)
        {
            printf("et_netclient: cannot open ET system\n");
            exit(1);
        }
        et_open_config_destroy(openconfig);

        {
            /* This could be confusing so here's a comment...
             * The output of this component is attached to the input of the next component in the
             * data-stream. So we find the input station and attach to it but the station id and attachment
             * are stored in the fields called output NOT the ones called input...
            */

            char station_name[100];
            sprintf(station_name,"%s_input",myname);
            if (et_station_name_to_id(id,&sender_id->output_et_station,station_name) < 0)
            {
                EMU_DEBUG(("error finding station %s",station_name));
                exit(1);
            }

            if (et_station_attach(id, sender_id->output_et_station, &sender_id->output_et_att) < 0)
            {
                printf("et_netclient: error in station attach\n");
                exit(1);
            }

        }
        sender_id->the_et_id = id;
    }



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


void emu_create_send_thread(emu_sender_id sender_id)
{

    int status;

    EMU_DEBUG (("Create send thread with sender_id %08x type %d", sender_id,sender_id->type));

    // The send thread will run forever unless there is an error or the keep_going
    // flag is cleared.

    sender_id->keep_going = 1;

    switch (sender_id->type)
    {
    case FIFO_TYPE:
        EMU_DEBUG(("input is from a FIFO"));
        emu_create_thread(0,"FIFO send thread 0", emu_FIFO_send_thread, (void *) sender_id);
        emu_create_thread(0,"FIFO test thread 0", emu_FIFO_test_thread, (void *) sender_id);
        break;
    case ET_TYPE:
    default:
        EMU_DEBUG(("default input is from a ET"));
        emu_create_thread(0,"ET send thread 0", emu_ET_send_thread, (void *) sender_id);
        break;
    }

    return;
}

/*
 * author heyes
 *
 * The thread to do all of the work.
 */

void *emu_FIFO_send_thread(void *arg)
{
    struct emu_thread *thread_descriptor = (struct emu_thread *) arg;
    int status = 0;
    et_event   *pe[1000];
    int size, actual;
    emu_sender_id sender_id = (emu_sender_id) thread_descriptor->args;
    et_sys_id et_id = sender_id->the_et_id;
    et_att_id att = sender_id->output_et_att;

    printf("I am a thread named \"%s\" my sender_id is %08x input type is %d\n", thread_descriptor->name, sender_id, sender_id->type);

    while( sender_id->keep_going)
    {
        int count;
        status = et_station_getattachments(et_id,sender_id->output_et_station,&count);
        if (status != ET_OK)
        {
            printf("error in et_station_getattachments\n");
            break;
        }

        if (count == 0)
        {
            struct timespec waitforme;

            waitforme.tv_sec  = 1;
            waitforme.tv_nsec = 0;



            //EMU_DEBUG(("PUT Waiting for att\n" ))
            nanosleep(&waitforme, NULL);
            continue;
        }

        while( sender_id->keep_going && ( sender_id->input_fifo != NULL) && (status == 0))
        {
            // Wait for data on FIFO

            int i, waiting, queue_depth;
            char *data;
            //EMU_DEBUG(("get et event"));


            /* Get a record right at the start. We do this so that we block
             * here if there is no work to do.
             */

            data = get_cb_data(sender_id->input_fifo);

            if ((int) data == -1)
                break;

            /* Get the count of the number of other events waiting on the queue.
            */

            waiting = get_cb_count(sender_id->input_fifo);

            if (waiting == -1)
                break;

            /* Add the event we already popped off the queue.
            */

            waiting ++;

            while (waiting)
            {

                /* Ask the ET system for enough ET events to hold the number of records in the queue
                 * plus the one we just pulled off the top. ET returns an array of ET events that may
                 * be less than the number we asked for but holds at least one event.
                */

                status = et_events_get(et_id,att,pe, ET_SLEEP | ET_NOALLOC | ET_MODIFY, NULL,waiting,&actual);

                //printf("waiting = %d actual = %d\n",waiting,actual);

                if (status != ET_OK)
                {
                    printf("error in et_event_new\n");
                    break;
                }

                for (i =0;i<actual; i++)
                {
                    /* The first time that we come into this loop data != NULL because we already popped an
                     * event off the queue. Next time around the loop data == NULL because we set it to NULL
                     * at the bottom of the loop.
                    */

                    if (data == NULL)
                        data = get_cb_data(sender_id->input_fifo);

                    if ((int) data == -1)
                        break;

                    // Convert data to a record
                    emu_data_record_ptr record = (emu_data_record_ptr) data;

                    //EMU_DEBUG(("GOT Data %08x, len = %d, pdata %08x",record,record->record_data.length,pe[i]->pdata ))

                    free(pe[i]->pdata);

                    pe[i]->pdata = record;
                    pe[i]->length = record->record_header.length;
                    pe[i]->control[0] = sender_id->output_et_station+1;

                    // EMU_DEBUG(("put et event\n \t control %d \n\t  owner %d \n\t NOALLOC %d \n\t modify %d \n\t MODIFY_HEADER %d", pe[i]->control[0],pe[i]->owner,ET_NOALLOC,pe[i]->modify,ET_MODIFY_HEADER ));
                    pe[i]->owner = ET_NOALLOC;
                    //pe[i]->modify = ET_MODIFY;
                    //pe[i]->modify = 0;
                    data = NULL;
                    // EMU_DEBUG(("put et event control %d,%d,%d,%d"));
                }

                //EMU_DEBUG(("put et event"));

                status = et_events_put(et_id,att, pe,actual);
                if (status != ET_OK)
                {
                    printf("et_producer: put error\n");
                    break;
                }
                waiting = waiting - actual;
            }
            //EMU_DEBUG(("done"));

        }
    }
    sender_id->keep_going = 0;
    {
        /* If we quit this thread then we have to empty the input FIFO onto the floor
         * until the thread putting events in notices that we are exiting.
        *
        */

        delete_cb(sender_id->input_fifo);

    }

clean_exit_emu_send_thread:
    emu_thread_cleanup(thread_descriptor);
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

void *emu_FIFO_test_thread(void *arg)
{
    struct emu_thread *thread_descriptor = (struct emu_thread *) arg;
    int counter, status = 0;
    int recordCounter = 0;
    circ_buf_t *fifo;

    emu_sender_id sender_id = (emu_sender_id) thread_descriptor->args;
    fifo = sender_id->input_fifo;

    printf("I am a thread named \"%s\" my sender_id is %08x input type is %d\n", thread_descriptor->name, sender_id, sender_id->type);
    while( sender_id->keep_going && ( sender_id->input_fifo != NULL) && (status == 0))
    {
        struct timespec waitforme;

        /* set some useful timeout periods */
        waitforme.tv_sec  = 0;
        waitforme.tv_nsec = 50000000; /* 50 millisec */

        // Convert data to a record
        emu_data_record_ptr record = (emu_data_record_ptr) &dummy_records[recordCounter & 0xFF][0];
        record->record_header.length = 500;
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
        //nanosleep(&waitforme, NULL);
        put_cb_data(fifo,(void *) record);
    }

clean_exit_emu_send_thread:
    emu_thread_cleanup(thread_descriptor);
    return;
}

/*
 * author heyes
 *
 * The thread to do all of the work.
 */

void *emu_ET_send_thread(void *arg)
{
    struct emu_thread *thread_descriptor = (struct emu_thread *) arg;

    emu_sender_id sender_id = (emu_sender_id) thread_descriptor->args;

    printf("I am a thread named \"%s\" my sender_id is  %08x input type is %d\n", thread_descriptor->name, sender_id, sender_id->type);

clean_exit_emu_send_thread:
    EMU_DEBUG(("free %08x",sender_id));
    emu_thread_cleanup(thread_descriptor);
    return;
}

