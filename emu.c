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

#define EMU_MAIN

#include "emu.h"
#include "generic_key_control.h"

emu_config_ptr emu_configuration;

static char *help_string =  "usage: emu -v -n name [-o name]\n"
                            "      -v for verbose output\n"
                            "      -n EMU name\n"
                            "      -i input\n"
                            "      -S sender simulation\n"
                            "      -R reader simulation\n"
                            "      -P process simulation\n"
                            "      -p port for ET to listen on \n"
                            "      -o name of EMU this EMU sends to\n";

static void print_help()
{
    fprintf(stderr, "%s", help_string);
}

emu_config_ptr emu_config ()
{
    return emu_configuration;
}

int emu_initialize(int argc,char **argv)
{
    int           c;
    extern char  *optarg;
    extern int    optind;

    emu_configuration = malloc(sizeof(emu_config_t));

    bzero(emu_configuration, sizeof(emu_config_t));
    emu_configuration->reader_mode = EMU_MODE_NORMAL;
    while ((c = getopt(argc, argv, "hv:n:i:o:p:")) != EOF)
    {
        switch (c)
        {

        case 'n':
            /* if (strlen(optarg) >= EMU_NAME_LENGTH)
             {
                 fprintf(stderr, "EMU name is too long\n");
                 return ERROR;
             }*/

            emu_configuration->emu_name = strdup(optarg);
            break;

        case 'o':
            /*if (strlen(optarg) >= EMU_NAME_LENGTH)
        {
                fprintf(stderr, "Target EMU name is too long\n");
                return ERROR;
        }*/

            emu_configuration->output_target_name = strdup(optarg);
            break;
        case 'i':
            if (strchr(optarg,':') == NULL)
            {
                emu_configuration->input_names[emu_configuration->input_count] = strdup(optarg);
                emu_configuration->input_count++;
            }
            else
            {
                char *input_list = strdup(optarg);
                char *found = strrchr(input_list,':');

                while (1)
                {
                    emu_configuration->input_names[emu_configuration->input_count] = strdup(found+1);
                    emu_configuration->input_count++;
                    *found = '\0';
                    found = strrchr(input_list,':');
                    if (found==NULL)
                    {
                        emu_configuration->input_names[emu_configuration->input_count] = strdup(input_list);
                        emu_configuration->input_count++;
                        break;
                    }
                }
                free(input_list);
            }
            break;
        case 'R':
            emu_configuration->reader_mode = EMU_MODE_SIMULATE;
            break;
        case 'P':
            emu_configuration->process_mode = EMU_MODE_SIMULATE;
            break;
        case 'S':
            emu_configuration->sender_mode = EMU_MODE_SIMULATE;
            break;
        case 'p':
            emu_configuration->port = atoi(optarg);
            break;
        case ':':
        case 'h':
        case '?':
        default:
            print_help();

        }
    }

    printf ("optind %d argc %d\n", optind, argc);

    if ((argc == 1) || (optind < argc))
    {
        print_help();
        return ERROR;
    }

}
int emu_quit()
{
    if(emu_configuration->read != NULL)
        emu_reader_stop(emu_configuration->read);

    if(emu_configuration->process != NULL)
        emu_process_stop(emu_configuration->process);

    if(emu_configuration->send != NULL)
        emu_sender_stop(emu_configuration->send);

    exit(0);
}

int main(int argc,char **argv)
{
    // block all signals
    esh_create();
    esh_start();

    GKB_add_key('h', GKB_print_help, NULL,"Print out this help message");

    GKB_add_key('q', emu_quit, NULL, "quit");
    GKB_add_key('t',emu_thread_list,NULL,"list threads");
    GKB_start();

    if (emu_initialize(argc, argv) == ERROR)
    {
        return ERROR;
    }

    printf("NAME is %s\n", emu_configuration->emu_name);

    emu_init_thread_system();

    // Initialize routines look at the config and return null if they are not needed.

    emu_configuration->read = emu_reader_initialize();
    emu_configuration->process = emu_process_initialize();
    emu_configuration->send = emu_sender_initialize();

    if(emu_configuration->read != NULL)
        emu_reader_start(emu_configuration->read);

    if(emu_configuration->process != NULL)
        emu_process_start(emu_configuration->process);

    if(emu_configuration->send != NULL)
        emu_sender_start(emu_configuration->send);

    // The thread monitor is a thread that scans the list of threads
    // and reaps dead ones.
    emu_thread_monitor();

}


