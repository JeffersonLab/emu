/*----------------------------------------------------------------------------
 *  Copyright (c) 2007        Southeastern Universities Research Association,
 *                            Thomas Jefferson National Accelerator Facility
 *
 *    This software was developed under a United States Government license
 *    described in the NOTICE file included as part of this distribution.
 *
 *    Author:  heyes
 *    Created: Mar 26, 2007
 *
 *             heyes@jlab.org                    Jefferson Lab, MS-12H
 *             Phone: (757) 269-7030             12000 Jefferson Ave.
 *             Fax:   (757) 269-5800             Newport News, VA 23606
 *
 *----------------------------------------------------------------------------
 *
 * Description:
 *      emu  - emu_reader.c
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

#include "emu_reader.h"


static void emu_signal_handler(void *arg)
{
    sigset_t       sigwaitset;
    int status, sig_num;
    struct emu_thread *thread_descriptor = (struct emu_thread *) arg;

    sigemptyset(&sigwaitset);
    sigaddset(&sigwaitset, SIGINT);
    /* turn this thread into a signal handler */
    sigwait(&sigwaitset, &sig_num);

    printf("Interrupted by CONTROL-C\n");
    printf("ET %08x is exiting\n",thread_descriptor->args);
    et_system_close(((et_sys_id *)thread_descriptor->args));

    emu_thread_cleanup(thread_descriptor);
    exit(1);
}


static void emu_dummy_build_thread(void *arg)
{
    struct emu_thread *thread_descriptor = (struct emu_thread *) arg;
    emu_reader_id reader_id = thread_descriptor->args;
    et_sys_id id = reader_id->id;
    et_id      *etid = (et_id *) id;

    int station_counter = 0;

    EMU_DEBUG(("\nATTACH HANDLER"))

    while(et_alive(id))
    {
        struct timespec waitforme;
        int stat_id,status;
        int         num;
        et_station *ps;

        waitforme.tv_sec  = 1;
        waitforme.tv_nsec = 0;


        //printf("Max number of stations %d\n",etid->sys->config.nstations);

        for (num=0; num < reader_id->number_inputs ; num++)
        {
            et_station *ps = etid->grandcentral+reader_id->inputs[num].output_station;

            if (ps->data.status == ET_STATION_ACTIVE)
            {

                et_event   *pe[1000];
                int i, ix, actual = 0;

                stat_id = num;

               // EMU_DEBUG(("Foundactive Station %s at position %d",ps->name ,stat_id));

                status = et_events_get( etid,reader_id->inputs[num].output_att, pe, ET_SLEEP , NULL,10,&actual);

                if (status != ET_OK)
                {
                    printf("error in et_event_new\n");
                    goto error;
                }

                for (i =0;i<actual; i++)
                {
                	emu_data_record_ptr record = (emu_data_record_ptr) pe[i]->pdata;

                	printf ("Got a record number %d from input %d\n",record->record_header.recordNB, record->record_header.rocID);
                	printf ("   length is %d\n", record->record_data.length);
                	for (ix=0;ix<10;ix++) {
                		printf("     data[%2d] - %08X\n",ix, record->record_data.data[ix]);
                	}
                	printf ("---------------------\n\n");
                    pe[i]->length = 0;
                    pe[i]->control[0] =reader_id->inputs[num].input_station;
                    pe[i]->owner = reader_id->gc_att;
                }

                //EMU_DEBUG(("put et event"));

                status = et_events_put(etid,reader_id->gc_att, pe,actual);

                if (status != ET_OK)
                {
                    printf("et_producer: put error\n");
                    goto error;
                }
            }

        }

        nanosleep(&waitforme, NULL);
        //EMU_DEBUG(("Waiting for a station to appear\n" ))
    }
error:
    emu_thread_cleanup(thread_descriptor);
      exit(1);
}


/*
 * emu emu_reader.c
 * Function reader_initialize station
 *
 *
 */

static void reader_initialize (emu_reader_id reader_id,char *name)
{

    et_statconfig	 sconfig;
    int rid = reader_id->number_inputs;
    int status;
    et_stat_id	 my_stat;
    char station_name[100];
    int selection[4] = {0,-1,-1,-1};

    /* We create two stations. The first holds our empty events the second our full ones.
     * since both the consumer and the producer  need to attach to the input we use
     * ET_STATION_USER_MULTI
     */

    et_station_config_init(&sconfig);
    et_station_config_setuser(sconfig, ET_STATION_USER_MULTI);
    et_station_config_setrestore(sconfig, ET_STATION_RESTORE_OUT);
    et_station_config_setprescale(sconfig, 1);
    et_station_config_setcue(sconfig, 100);
    et_station_config_setselect(sconfig, ET_STATION_SELECT_MATCH);
    et_station_config_setblock(sconfig, ET_STATION_BLOCKING);

    // create a station to hold empty events
    sprintf(station_name,"%s_input",name);
    if ((status = et_station_create(reader_id->id, &my_stat, station_name, sconfig)) < ET_OK)
    {
        goto station_error;
    }
    selection[0] = my_stat;
    et_station_setselectwords(reader_id->id, my_stat,selection);

    reader_id->inputs[rid].input_station = my_stat;
    // create a station to hold filled events

    //et_station_config_setselect(sconfig, ET_STATION_SELECT_ALL);
    et_station_config_setuser(sconfig, ET_STATION_USER_MULTI);

    sprintf(station_name,"%s_output",name);
    if ((status = et_station_create(reader_id->id, &my_stat, station_name, sconfig)) < ET_OK)
    {
        goto station_error;
    }

    selection[0] = my_stat;
    et_station_setselectwords(reader_id->id, my_stat,selection);

    reader_id->inputs[rid].output_station = my_stat;

    /* Now attach to the stations that we just created.
    */

    if (et_station_attach(reader_id->id, reader_id->inputs[rid].input_station, &reader_id->inputs[rid].input_att) < 0)
    {
        printf("et_netclient: error in station attach\n");
        exit(1);
    }

    if (et_station_attach(reader_id->id, reader_id->inputs[rid].output_station, &reader_id->inputs[rid].output_att) < 0)
    {
        printf("et_netclient: error in station attach\n");
        exit(1);
    }


    reader_id->number_inputs++;

    {
        /* Now to prime the pump by putting EMU_READER_QUEUE_SIZE events into the pool.
        */

        int i, actual;
        et_event   *pe[EMU_READER_QUEUE_SIZE];
        status = et_events_new(reader_id->id,reader_id->gc_att,pe, ET_SLEEP, NULL,0,EMU_READER_QUEUE_SIZE,&actual);

        if (status != ET_OK)
        {
            printf("error in et_event_new\n");
            goto station_error;
        }

        for (i =0;i<actual; i++)
        {
            //pe[i]->pdata = NULL;
            pe[i]->length = 0;
            pe[i]->control[0] =reader_id->inputs[rid].input_station;

            //EMU_DEBUG(("put et event control %d,%d,%d,%d"));
        }

        EMU_DEBUG(("put et event"));

        status = et_events_put(reader_id->id,reader_id->gc_att, pe,actual);
        if (status != ET_OK)
        {
            printf("et_producer: put error\n");
            goto station_error;
            ;
        }

    }


    return;

station_error:

    if (status == ET_ERROR_EXISTS)
    {
        /* my_stat contains pointer to existing station */;
        printf("et_netclient: station already exists\n")
        ;
    }
    else if (status == ET_ERROR_TOOMANY)
    {
        printf("et_netclient: too many stations created\n")
        ;
        exit(1);
    }
    else if (status == ET_ERROR_REMOTE)
    {
        printf("et_netclient: memory or improper arg problems\n")
        ;
        exit(1);
    }
    else if (status == ET_ERROR_READ)
    {
        printf("et_netclient: network reading problem\n")
        ;
        exit(1);
    }
    else if (status == ET_ERROR_WRITE)
    {
        printf("et_netclient: network writing problem\n")
        ;
        exit(1);
    }
    else
    {
        printf("et_netclient: error in station creation\n");
        exit(1);
    }

}

/*
 * emu emu_reader.c
 * Function emu_main
 *
 *
 */

void emu_initialize_reader ( char *et_filename)
{
    emu_reader_id reader_id;
    int           errflg = 0;
    int           i_tmp;

    int           status;
    int           et_verbose = ET_DEBUG_NONE;
    int           deleteFile = 0;

    sigset_t      sigblockset;
    et_statconfig sconfig;
    et_sysconfig  config;
    et_stat_id    statid;
    et_sys_id     id;

    reader_id = malloc(sizeof( struct emu_reader));

    bzero(reader_id, sizeof( struct emu_reader));

    /*************************/
    /* setup signal handling */
    /*************************/
    sigfillset(&sigblockset);
    status = pthread_sigmask(SIG_BLOCK, &sigblockset, NULL);
    if (status != 0)
    {
        printf("pthread_sigmask failure\n");
        exit(1);
    }
    /************************************/
    /* default configuration parameters */
    /************************************/
    int nevents = 2000;               /* total number of events */
    int event_size = 3000;            /* size of event in bytes */

    EMU_DEBUG(("asking for %d byte events.", event_size))
    EMU_DEBUG(("asking for %d events.", nevents))

    remove(et_filename);


    /********************************/
    /* set configuration parameters, MOVE THIS SOMEWHERE ELSE*/
    /********************************/

    if (et_system_config_init(&config) == ET_ERROR)
    {
        printf("et_start: no more memory\n");
        exit(1);
    }
    /* total number of events */
    et_system_config_setevents(config, nevents);

    /* size of event in bytes */
    et_system_config_setsize(config, event_size);

    /* max number of temporary (specially allocated mem) events */
    /* This cannot exceed total # of events                     */
    et_system_config_settemps(config, nevents);

    /* limit on # of stations */
    et_system_config_setstations(config, ET_ATTACHMENTS_MAX);

    /* soft limit on # of attachments (hard limit = ET_ATTACHMENTS_MAX) */
    et_system_config_setattachments(config, 200);

    /* soft limit on # of processes (hard limit = ET_PROCESSES_MAX) */
    et_system_config_setprocs(config, ET_PROCESSES_MAX);


    /* Make sure filename is null-terminated string */
    if (et_system_config_setfile(config, et_filename) == ET_ERROR)
    {
        printf("bad filename argument\n");
        exit(1);
    }



    /*************************/
    /*    start ET system    */
    /*************************/

    EMU_DEBUG(("starting ET system %s\n", et_filename))

    if (et_system_start(&id, config) != ET_OK)
    {
        printf("error in starting ET system");
        exit(1);
    }

    EMU_DEBUG(("ET system %s started %08x", et_filename,id))

    emu_create_thread(0,"Control-C handler", emu_signal_handler, (void *) id);

    et_system_setdebug(id, ET_DEBUG_INFO);

    reader_id->id = id;

    if (et_station_attach(id, ET_GRANDCENTRAL, &reader_id->gc_att) < 0)
    {
        printf("et_netclient: error in station attach\n");
        exit(1);
    }

    emu_create_thread(0,"Attach handler", emu_dummy_build_thread, (void *) reader_id);
    reader_initialize (reader_id,"ROC1");
//    reader_initialize (reader_id,"ROC2");
//	reader_initialize (reader_id,"ROC3");
//	reader_initialize (reader_id,"ROC4");
//	reader_initialize (reader_id,"ROC5");
//	reader_initialize (reader_id,"ROC6");
//	reader_initialize (reader_id,"ROC7");
//	reader_initialize (reader_id,"ROC8");
//	reader_initialize (reader_id,"ROC9");
//	reader_initialize (reader_id,"ROC10");
//	reader_initialize (reader_id,"ROC11");
//	reader_initialize (reader_id,"ROC12");
//	reader_initialize (reader_id,"ROC13");
//	reader_initialize (reader_id,"ROC14");
//	reader_initialize (reader_id,"ROC15");
//	reader_initialize (reader_id,"ROC16");
//	reader_initialize (reader_id,"ROC17");
//	reader_initialize (reader_id,"ROC18");
//	reader_initialize (reader_id,"ROC19");
//	reader_initialize (reader_id,"ROC20");

    return;
}


