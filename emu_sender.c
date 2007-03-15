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
 *      emu  - emu_send_thread.c
 *
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

static unsigned long dummy_records[10][10];
/*
 * emu emu_send_thread.c
 * Function emu_initialize_sender
 *
 *
 *
 */

emu_sender_id emu_initialize_sender (int type, char *target)
{
    int status;
    emu_sender_id sender_id = (emu_sender_id) malloc(sizeof(emu_send_thread_args));
    bzero(sender_id, sizeof(emu_send_thread_args));

    sender_id->fifo_thread_args.type = type;
    sender_id->generic_args.target = strdup(target);

    switch (type)
    {
    case FIFO_TYPE:
        {
            circ_buf_t *fifo;
            EMU_DEBUG(("input is from a FIFO sender_id %08x",sender_id));
            fifo = new_cb("EMU input FIFO");

            sender_id->fifo_thread_args.input_fifo = fifo;

            break;
        }
    case ET_TYPE:
    default:
        EMU_DEBUG(("default input is from a ET called %s", sender_id->generic_args.target));
        break;
    }

    /*
     * Both types of sender send their data to an ET system.
    	 */
    {
        et_sys_id	 id;
        et_statconfig	 sconfig;
        et_stat_id	 my_stat;
        et_att_id	 my_att;
        et_openconfig  openconfig;

        et_open_config_init(&openconfig);
        et_open_config_setmode(openconfig, ET_HOST_AS_REMOTE);

        //et_open_config_setcast(openconfig, ET_BROADANDMULTICAST);
        //et_open_config_addmulticast(openconfig, ET_MULTICAST_ADDR);
        //et_open_config_setTTL(openconfig, 2);


        /* For direct access to a remote ET system, use ET_DIRECT
         * and specify the port that the server is on.

          et_open_config_setcast(openconfig, ET_DIRECT);
          et_open_config_sethost(openconfig, "alumina.jlab.org");
          et_open_config_setserverport(openconfig, 11111);
*/

        if (et_open(&id, sender_id->generic_args.target, openconfig) != ET_OK)
        {
            printf("et_netclient: cannot open ET system\n");
            exit(1);
        }
        et_open_config_destroy(openconfig);

        /* define a station */
        et_station_config_init(&sconfig);
        et_station_config_setuser(sconfig, ET_STATION_USER_MULTI);
        et_station_config_setrestore(sconfig, ET_STATION_RESTORE_OUT);
        et_station_config_setprescale(sconfig, 1);
        et_station_config_setcue(sconfig, 100);
        et_station_config_setselect(sconfig, ET_STATION_SELECT_ALL);
        et_station_config_setblock(sconfig, ET_STATION_NONBLOCKING);

        /* create a station */
        if ((status = et_station_create(id, &my_stat, "A_station", sconfig)) < ET_OK)
        {
            if (status == ET_ERROR_EXISTS)
            {
                /* my_stat contains pointer to existing station */;
                printf("et_netclient: station already exists\n");
            }
            else if (status == ET_ERROR_TOOMANY)
            {
                printf("et_netclient: too many stations created\n");
                exit(1);
            }
            else if (status == ET_ERROR_REMOTE)
            {
                printf("et_netclient: memory or improper arg problems\n");
                exit(1);
            }
            else if (status == ET_ERROR_READ)
            {
                printf("et_netclient: network reading problem\n");
                exit(1);
            }
            else if (status == ET_ERROR_WRITE)
            {
                printf("et_netclient: network writing problem\n");
                exit(1);
            }
            else
            {
                printf("et_netclient: error in station creation\n");
                exit(1);
            }
        }

        et_station_config_destroy(sconfig);

        /* create an attachment */

        //if (et_station_attach(id, my_stat, &sender_id->generic_args.output_et_att) < 0)
        if (et_station_attach(id, ET_GRANDCENTRAL, &sender_id->generic_args.output_et_att) < 0)
        {
            printf("et_netclient: error in station attach\n");
            exit(1);
        }

        sender_id->generic_args.output_et_id = id;

    }



    return sender_id;
error:
    free(sender_id);
    return NULL;
}


void emu_create_send_thread(emu_sender_id sender_id)
{

    int status;

    EMU_DEBUG (("Create send thread with sender_id %08x type %d", sender_id,sender_id->fifo_thread_args.type));

    sender_id->et_thread_args.keep_going = 1;

    switch (sender_id->fifo_thread_args.type)
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

    emu_sender_id sender_id = (emu_sender_id) thread_descriptor->args;

    printf("I am a thread named \"%s\" my sender_id is %08x input type is %d\n", thread_descriptor->name, sender_id, sender_id->fifo_thread_args.type);
    while( sender_id->fifo_thread_args.keep_going && ( sender_id->fifo_thread_args.input_fifo != NULL) && (status == 0))
    {
        // Wait for data on FIFO
        void *data = get_cb_data(sender_id->fifo_thread_args.input_fifo);

        // Convert data to a record
        roc_record record = (roc_record) data;
        EMU_DEBUG(("GOT Data %08x, len = %d",record,record->recordNB ))
        status = emu_generic_sender(sender_id, data);
    }

clean_exit_emu_send_thread:
    free(sender_id->generic_args.target);
    free(sender_id);
    emu_thread_cleanup(thread_descriptor);
    return;
}

void *emu_FIFO_test_thread(void *arg)
{
    struct emu_thread *thread_descriptor = (struct emu_thread *) arg;
    int status = 0;
    circ_buf_t *fifo;

    dummy_records[0][0] = 4;
    dummy_records[1][0] = 4;
    dummy_records[2][0] = 4;
    dummy_records[3][0] = 4;
    emu_sender_id sender_id = (emu_sender_id) thread_descriptor->args;
    fifo = sender_id->fifo_thread_args.input_fifo;

    printf("I am a thread named \"%s\" my sender_id is %08x input type is %d\n", thread_descriptor->name, sender_id, sender_id->fifo_thread_args.type);
    while( sender_id->fifo_thread_args.keep_going && ( sender_id->fifo_thread_args.input_fifo != NULL) && (status == 0))
    {



        EMU_DEBUG(("PUT Data %08x, len = %d",&dummy_records[0][0],dummy_records[0][0] ))
        put_cb_data(fifo,(void *) &dummy_records[0][0]);
        put_cb_data(fifo,(void *) &dummy_records[1][0]);
        put_cb_data(fifo,(void *) &dummy_records[2][0]);
        put_cb_data(fifo,(void *) &dummy_records[3][0]);
    }

clean_exit_emu_send_thread:
    free(sender_id->generic_args.target);
    free(sender_id);
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

    printf("I am a thread named \"%s\" my sender_id is  %08x input type is %d\n", thread_descriptor->name, sender_id, sender_id->fifo_thread_args.type);

clean_exit_emu_send_thread:
    EMU_DEBUG(("free %08x",sender_id));
    free(sender_id);
    emu_thread_cleanup(thread_descriptor);
    return;
}

int emu_generic_sender(emu_sender_id sender_id, roc_record data)
{
    et_event   *pe;
    int size, status;
    EMU_DEBUG(("get et event"));

    status = et_event_new(sender_id->generic_args.output_et_id,sender_id->generic_args.output_et_att,&pe, ET_SLEEP | ET_NOALLOC, NULL, size);
    if (status != ET_OK)
    {
        printf("et_producer: error in et_event_new\n");
        return 1;
    }
    EMU_DEBUG(("put et event"));
    pe->pdata = (char *) data;
    pe->length = 4;
    status = et_event_put(sender_id->generic_args.output_et_id,sender_id->generic_args.output_et_att, pe);
    if (status != ET_OK)
    {
        printf("et_producer: put error\n");
        return 1;
    }
        EMU_DEBUG(("done"));
    return 0;
}
