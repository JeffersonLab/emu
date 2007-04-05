/*----------------------------------------------------------------------------*
 *  Copyright (c) 1998        Southeastern Universities Research Association, *
 *                            Thomas Jefferson National Accelerator Facility  *
 *                                                                            *
 *    This software was developed under a United States Government license    *
 *    described in the NOTICE file included as part of this distribution.     *
 *                                                                            *
 *    Author:  Carl Timmer                                                    *
 *             timmer@jlab.org                   Jefferson Lab, MS-12H        *
 *             Phone: (757) 269-5130             12000 Jefferson Ave.         *
 *             Fax:   (757) 269-5800             Newport News, VA 23606       *
 *                                                                            *
 *----------------------------------------------------------------------------*
 *
 * Description:
 *      ET system sample event client
 *
 *----------------------------------------------------------------------------*/

#include <stdio.h>
#include <stdlib.h>
#include "et.h"

#include "emu_common.h"
#include "emu_system_init.h"
#include "emu_sender.h"


int main(int argc,char **argv)
{
    int           c;
    extern char  *optarg;
    extern int    optind;
    char *et_filename = NULL;

    while ((c = getopt(argc, argv, "v:f:")) != EOF)
    {
        switch (c)
        {

        case 'f':
            if (strlen(optarg) >= ET_FILENAME_LENGTH)
            {
                fprintf(stderr, "ET file name is too long\n");
                exit(-1);
            }

            et_filename = strdup(optarg);
            break;

        case ':':
        case 'h':
        case '?':
        default:
            fprintf(stderr, "usage: %s -v [-f file]\n", argv[0]);
            fprintf(stderr, "          -v for verbose output\n");
            fprintf(stderr, "          -f sets memory-mapped file name\n");

        }
    }

    if (optind < argc)
    {
        fprintf(stderr, "usage: %s -v [-f file]\n", argv[0]);
        fprintf(stderr, "          -v for verbose output\n");
        fprintf(stderr, "          -f sets memory-mapped file name\n");
        exit(2);
    }

    emu_init_thread_system();

    emu_initialize_reader (et_filename);
    // The thread monitor is a thread that scans the list of threads
    // and reaps dead ones.
    emu_thread_monitor();

}


