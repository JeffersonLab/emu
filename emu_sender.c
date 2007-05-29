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
#include "emu.h"

static unsigned long dummy_records[256][10000];

static void interrupt_signal_handler(void *arg)
{
    emu_sender_id sender_id = (emu_sender_id) arg;

    printf("Sender Interrupted by CONTROL-C\n");
    emu_sender_stop(sender_id);
}

void *emu_sender_attach(void *arg)
{
    struct emu_thread *thread_descriptor = (struct emu_thread *) arg;
    int status = 0;
    et_event   *pe[1000];
    int size, actual;
    emu_sender_id sender_id = (emu_sender_id) thread_descriptor->args;
    et_sys_id	 id;
    et_stat_id	 my_stat;
    et_att_id	 my_att;
    et_openconfig  openconfig;
    int selections[] = {0,-1,-1,-1};
    struct timespec time_to_wait;

    /* This thread takes care of the connection to ET. We are a process remote from the ET so
     * everything happens over  the network.
    */
	printf("here\n");
	printf("sender_id->target %s\n", sender_id->target);
	printf("there\n");
    // Thread lives until told to die
    while (sender_id->keep_going)
    {
        // Have we been here before?

        if (sender_id->the_et_id == NULL )
        {

            /*
             * We Initialize the output ET system and tell it that it is just
             * a "front" for a remote system.
             */

            et_open_config_init(&openconfig);
            /*et_open_config_setmode(openconfig, ET_HOST_AS_REMOTE);
            et_open_config_sethost(openconfig,ET_HOST_ANYWHERE);
            et_open_config_setcast(openconfig, ET_MULTICAST);
            et_open_config_setmultiport(openconfig, ET_MULTICAST_PORT);
            et_open_config_addmulticast(openconfig, ET_MULTICAST_ADDR);
            et_open_config_addmulticast(openconfig, "239.111.222.0");*/

            et_open_config_setmode(openconfig, ET_HOST_AS_REMOTE);
            et_open_config_setcast(openconfig, ET_BROADCAST);
            et_open_config_sethost(openconfig,ET_HOST_ANYWHERE);
            et_open_config_setport(openconfig,ET_BROADCAST_PORT);
            et_open_config_addbroadcast(openconfig,"129.57.31.255");
            et_open_config_addbroadcast(openconfig,"129.57.29.255");
            et_open_config_setTTL(openconfig, 2);

            et_open_config_setwait(openconfig,ET_OPEN_WAIT);
            et_open_config_settimeout(openconfig,time_to_wait);


            /* For direct access to a remote ET system, use ET_DIRECT
             * and specify the port that the server is on.
             */

            //et_open_config_setcast(openconfig, ET_DIRECT);
            //et_open_config_sethost(openconfig, "albanac.jlab.org");

            //et_open_config_addbroadcast(openconfig,"129.57.31.255");
            //et_open_config_setport(openconfig, sender_id->port);

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
                sprintf(station_name,"%s_input",emu_config()->emu_name);
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
        else
        {
            emu_sleep(2);
        }
    }
    emu_thread_cleanup(thread_descriptor);
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

emu_sender_id emu_sender_initialize ()
{
    int status;
    emu_sender_id sender_id;
    if (emu_config()->output_target_name == NULL)
    {
        return NULL;
    }

    sender_id = (emu_sender_id) malloc(sizeof(emu_send_thread_args));
    bzero(sender_id, sizeof(emu_send_thread_args));

    sender_id->type = FIFO_TYPE;

    {

        if (strchr(emu_config()->output_target_name,':') != NULL)
        {
            sender_id->target  = strdup(emu_config()->output_target_name);
            *strchr(sender_id->target,':') = '\0';
            sender_id->port = atoi(strdup(strchr(emu_config()->output_target_name,':')+1));
        }
        else
        {
            sender_id->port = 11111;
            sender_id->target = strdup(emu_config()->output_target_name);

        }
        printf ("target = %s port= %d\n",sender_id->target,sender_id->port);
    }


    {
        circ_buf_t *fifo;

        if (emu_configuration->process != NULL)
        {
            fifo = emu_configuration->process->output;
        }
        else
        {
            fifo = new_cb("EMU input FIFO");
        }
        sender_id->input_fifo = fifo;
        sender_id->etmt_fifo = new_sized_cb("ET new event fifo",EMU_SENDER_QSIZE);
    }

    esh_block();
    esh_add("sender Control-C handler", interrupt_signal_handler, (void *) sender_id);

    return sender_id;
error:
    free(sender_id);
    return NULL;
}

static int emu_sender_simulate_pause(emu_sender_id sender_id)
{
    sender_id->pause = TRUE;
}
static int emu_sender_simulate_go(emu_sender_id sender_id)
{
    sender_id->pause = FALSE;
}
/*
 * emu emu_sender.c
 * Function emu_create_send_thread
 *
 * This function creates the thread that does all the work.
 * For debugging purposes it can also create a test thread for FIFO mode
 * that loops poking dummy records into the input FIFO.
 */


void emu_sender_start(emu_sender_id sender_id)
{

    int status;

    EMU_DEBUG (("Create send thread with sender_id %08x type %d", sender_id,sender_id->type));

    // The send thread will run forever unless there is an error or the keep_going
    // flag is cleared.

    sender_id->keep_going = 1;
    /*
     * Both types of sender send their data to an ET system.
     */

    sender_id->sender = emu_create_thread(1,"ET connector thread 0", emu_sender_attach, (void *) sender_id);

    switch (sender_id->type)
    {
    case FIFO_TYPE:
        EMU_DEBUG(("input is from a FIFO"));
        sender_id->sender = emu_create_thread(1,"FIFO send thread 0", emu_sender_process, (void *) sender_id);
        sender_id->getter = emu_create_thread(1,"ET buffer thread 0", emu_sender_etmtfifo, (void *) sender_id);
        sender_id->pause = TRUE;
        if (emu_configuration->process == NULL)
        {
            GKB_add_key('g', emu_sender_simulate_go, (void *) sender_id,"start data taking (go)");
            GKB_add_key('p', emu_sender_simulate_pause, (void *) sender_id, "pause data taking");
            sender_id->tester = emu_create_thread(1,"FIFO test thread 0", emu_sender_simulate, (void *) sender_id);
        }
        break;
    case ET_TYPE:
    default:
        EMU_DEBUG(("default input is from a ET"));
        //emu_create_thread(1,"ET send thread 0", emu_ET_send_thread, (void *) sender_id);
        break;
    }

    return;
}

void emu_sender_stop(emu_sender_id sender_id)
{
    circ_buf_t *fifo;
    printf("Stop Sender ET\n",sender_id);

    if (emu_configuration->process == NULL)
    {
        fifo = sender_id->input_fifo;
        sender_id->input_fifo =NULL;
        delete_cb(fifo);

        fifo = sender_id->etmt_fifo;
        sender_id->etmt_fifo =NULL;
        delete_cb(fifo);
    }
    sender_id->keep_going = 0;
    et_station_detach(sender_id->the_et_id,sender_id->output_et_att);
    printf("sender stopped\n");
}

void *emu_sender_etmtfifo(void *arg)
{

    struct emu_thread *thread_descriptor = (struct emu_thread *) arg;
    int status = 0;
    et_event   *pe[1000];
    int size, actual;
    emu_sender_id sender_id = (emu_sender_id) thread_descriptor->args;
    et_sys_id et_id;
    et_att_id att;
    int ix, count;

    while (sender_id->keep_going)
    {
        et_id = sender_id->the_et_id;
        att = sender_id->output_et_att;
        if (et_id == NULL)
        {
            printf("etmtfifo waiting for ET\n");
            emu_sleep(1);
            continue;
        }

        status = et_station_getattachments(et_id,sender_id->output_et_station,&count);
        if (status != ET_OK)
        {
            printf("error in et_station_getattachments\n");

            // instead of breaking do something smart here!!
            et_id = NULL;
            continue;
        }
        /* Ask the ET system for enough ET events to hold the number of records in the queue
          * plus the one we just pulled off the top. ET returns an array of ET events that may
          * be less than the number we asked for but holds at least one event.
          */
        count = EMU_SENDER_QSIZE;
        // - get_cb_count(sender_id->etmt_fifo);


        status = et_events_get(et_id,att,pe, ET_SLEEP | ET_NOALLOC | ET_MODIFY, NULL,count,&actual);

        //printf("count = %d actual = %d\n",count,actual);

        if (status != ET_OK)
        {
            printf("error in et_event_new\n");
            // instead of breaking do something smart here!!
            et_id = NULL;
            continue;
        }

        for (ix = 0; ix < actual;ix++)
            put_cb_data(sender_id->etmt_fifo, (void *) pe[ix]);
    }

}

/*
 * author heyes
 *
 * The thread to do all of the work.
 */

void *emu_sender_process(void *arg)
{
    struct emu_thread *thread_descriptor = (struct emu_thread *) arg;
    int status = 0;
    et_event   *pe[1000];
    int counter, ninput, nfree_et;
    emu_sender_id sender_id = (emu_sender_id) thread_descriptor->args;
    et_sys_id et_id;
    et_att_id att;

    printf("I am a thread named \"%s\" my sender_id is %08x input type is %d\n", thread_descriptor->name, sender_id, sender_id->type);

    counter = 0;

    while ( sender_id->keep_going)
    {
        if ( (counter == EMU_SENDER_QSIZE) || ((counter > 0) && ((get_cb_count(sender_id->etmt_fifo) == 0) || (get_cb_count(sender_id->input_fifo) == 0))))
        {
            et_id = sender_id->the_et_id;
            att = sender_id->output_et_att;
            if (et_id == NULL)
            {
                printf("sender waiting for ET\n");
                emu_sleep(1);
                continue;
            }

            //printf("sending with counter %d\n",counter);
            status = et_events_put(et_id,att, pe,counter);
            counter = 0;
            if (status != ET_OK)
            {
                printf("et_producer: put error\n");
                // instead of breaking do something smart here!!
                et_id = NULL;
                continue;
            }
        }

        emu_data_record_ptr record = (emu_data_record_ptr)  get_cb_data(sender_id->input_fifo);
        if ((int) record == -1)
            break;

        pe[counter] = (et_event *) get_cb_data(sender_id->etmt_fifo);
        if ((int) pe[counter] == -1)
            break;


        pe[counter]->pdata = record;
        pe[counter]->length = record->record_header.length;
        pe[counter]->control[0] = sender_id->output_et_station+1;
        if (emu_configuration->process == NULL)
            pe[counter]->owner = ET_NOALLOC;

        counter ++;
    }

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

void *emu_sender_simulate(void *arg)
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
        if (sender_id->pause)
        {
            emu_sleep(1);
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
    emu_thread_cleanup(thread_descriptor);
    return;
}



