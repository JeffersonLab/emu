/*----------------------------------------------------------------------------*
 *  Copyright (c) 2007        Southeastern Universities Research Association, *
 *                            Thomas Jefferson National Accelerator Facility  *
 *                                                                            *
 *    This software was developed under a United States Government license    *
 *    described in the NOTICE file included as part of this distribution.     *
 *                                                                            *
 *    Author:  Graham Heyes                                                   *
 *    Modification date : $Date: 2007-04-05 10:35:58 -0400 (Thu, 05 Apr 2007) $
 *    Revision : $Revision: 2489 $
 *    URL : $HeadURL: file:///daqfs/source/svnroot/coda/3.0/emu/emu_test_consumer.c $
 *             heyes@jlab.org                    Jefferson Lab, MS-12H        *
 *             Phone: (757) 269-7030             12000 Jefferson Ave.         *
 *             Fax:   (757) 269-5800             Newport News, VA 23606       *
 *                                                                            *
 *----------------------------------------------------------------------------*
 *
 * Description:
 *      Test EMU
 *		UNIX main program for EMU, not used for VxWorks.
 *----------------------------------------------------------------------------*/

#include "support/gll.h"
#include "support/gph.h"
#include "support/gkb.h"
#include "support/gtp.h"
#include "support/gparse.h"
#include <stdlib.h>
#include "emu_utilities.h"
extern void *emu_reader_initialize ();
extern void *emu_sender_initialize ();
extern void *emu_process_initialize ();

int emu_quit() {
    if(gph_get_value("/component/read/ID") != NULL)
        emu_reader_stop(gph_get_value("/component/read/ID"));

    if(gph_get_value("/component/proc/ID") != NULL)
        emu_process_stop(gph_get_value("/component/proc/ID"));

    if(gph_get_value("/component/send/ID") != NULL)
        emu_sender_stop(gph_get_value("/component/send/ID"));

    exit(0);
}

int main(int argc,char **argv) {
    // block all signals
    gsh_create();
    gsh_start();

    gkb_add_key('h', gkb_print_help, NULL,"Print out this help message");

    gkb_add_key('q', emu_quit, NULL, "quit");
    gkb_add_key('t',gtp_list,NULL,"list threads");
    gkb_add_key('l',gll_list,NULL,"list lists");

    if (gparse_file(argv[1]))
        exit(1);
    // for testing
    gph_list_parameters();
    //exit(0);
    gph_cmd_line(argc,argv);

    gkb_start();

    printf("NAME is %s\n", gph_get_param("/component/name"));

    // Initialize routines look at the config and return null if they are not needed.

    gph_add_value("/component/read/ID",emu_reader_initialize());

    if(gph_get_value("/component/read/ID") != NULL)
        emu_reader_start(gph_get_value("/component/read/ID"));

    gph_add_value("/component/proc/ID",emu_process_initialize());

    if(gph_get_value("/component/proc/ID") != NULL)
        emu_process_start(gph_get_value("/component/proc/ID"));

    gph_add_value("/component/send/ID",emu_sender_initialize());

    if(gph_get_value("/component/send/ID") != NULL)
        emu_sender_start(gph_get_value("/component/send/ID"));

    emu_wait_thread_end();


}


