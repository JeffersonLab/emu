/*----------------------------------------------------------------------------*
 *  Copyright (c) 2007        Southeastern Universities Research Association, *
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
     if (argc != 3)
    {
        printf("Usage: emu_test_producer <my name> <EMU name> \n");
        exit(1);
    }   //struct decoded_args main_args;

    EMU_DEBUG(("EMU tst event producer\n",1234));

    // decode_args(&main_args);

    EMU_DEBUG(("Initialize thread system\n"));

    // Start the EMU thread management system
    emu_init_thread_system();

    sender_id = emu_initialize_sender(FIFO_TYPE, argv[1],argv[2]);
    // Thread to take data from fifo/ET and send to downstream ET.
    emu_create_send_thread(sender_id);

    // The thread monitor is a thread that scans the list of threads
    // and reaps dead ones.
    emu_thread_monitor();

    exit(0);
}
