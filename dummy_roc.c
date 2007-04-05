/*----------------------------------------------------------------------------*
 *  Copyright (c) 1998        Southeastern Universities Research Association, *
 *                            Thomas Jefferson National Accelerator Facility  *
 *                                                                            *
 *    This software was developed under a United States Government license    *
 *    described in the NOTICE file included as part of this distribution.     *
 *                                                                            *
 *    Author:  Graham Heyes                                                   *
 *    Modification date : $Date$
 *    Revision : $Revision$
 *    URL : $HeadURL$
 *             heyes@jlab.org                    Jefferson Lab, MS-12H        *
 *             Phone: (757) 269-7030             12000 Jefferson Ave.         *
 *             Fax:   (757) 269-5800             Newport News, VA 23606       *
 *                                                                            *
 *----------------------------------------------------------------------------*
 *
 * Description:
 *      Dummy ROC style producer.
 *
 *----------------------------------------------------------------------------*/

#include <stdio.h>
#include <stdlib.h>
#include "et.h"

#include "emu_common.h"
#include "emu_system_init.h"
#include "emu_sender.h"

typedef struct
{
    int verbose;
}
decoded_args;

main(int argc,char **argv)
{
    emu_sender_id sender_id;
    //struct decoded_args main_args;

    EMU_DEBUG(("EMU tst event producer\n",1234));

    // decode_args(&main_args);

    EMU_DEBUG(("Initialize thread system\n"));

    // Start the EMU thread management system
    emu_init_thread_system();

    sender_id = emu_initialize_sender(FIFO_TYPE, "et_sys_ghtest");
    // Thread to take data from fifo/ET and send to downstream ET.
    emu_create_send_thread(sender_id);

    // The thread monitor is a thread that scans the list of threads
    // and reaps dead ones.
    emu_start_thread_monitor();
    printf("monitoring threads\n");

    emu_wait_thread_end();

#ifdef NEVER_DEFINED

    int        i, j, size, status, nevents_max, event_size;
    et_att_id  attach;
    et_sys_id  id;
    et_event   *pe;
    et_openconfig   openconfig;

    if (argc != 2)
    {
        printf("Usage: et_producer <et_filename>\n");
        exit(1);
    }

    /* set the desired size of our events in bytes */
    size = 10;

    /* opening the ET system is the first thing we must do */
    et_open_config_init(&openconfig);
    if (et_open(&id, argv[1], openconfig) != ET_OK)
    {
        printf("et_producer: et_open problems\n");
        exit(1);
    }
    et_open_config_destroy(openconfig);

    /* set the level of debug output that we want (everything) */
    et_system_setdebug(id, ET_DEBUG_INFO);

    /* attach to GRANDCENTRAL station since we are producing events */
    if (et_station_attach(id, ET_GRANDCENTRAL, &attach) < 0)
    {
        printf("et_producer: error in station attach\n");
        exit(1);
    }

    while (et_alive(id))
    {
        /* get new/unused event */
        status = et_event_new(id, attach, &pe, ET_SLEEP, NULL, size);
        if (status != ET_OK)
        {
            printf("et_producer: error in et_event_new\n");
            goto error;
        }

        /* put data into the event here */

        /* put event back into the ET system */
        status = et_event_put(id, attach, pe);
        if (status != ET_OK)
        {
            printf("et_producer: put error\n");
            goto error;
        }

        if (!et_alive(id))
        {
            et_wait_for_alive(id);
        }
    } /* while(alive) */

#endif
error:
    printf("dummy_event_sender: ERROR\n");
    exit(0);
}
