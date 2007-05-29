/*----------------------------------------------------------------------------
 *  Copyright (c) 2007        Southeastern Universities Research Association,
 *                            Thomas Jefferson National Accelerator Facility
 *
 *    This software was developed under a United States Government license
 *    described in the NOTICE file included as part of this distribution.
 *
 *    Author:  heyes
 *    Created: Mar 26, 2007
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
 *      emu  - emu_reader.c
 *
 *----------------------------------------------------------------------------*/

#include "emu.h"

static void interrupt_signal_handler(void *arg)
{
    emu_reader_id reader_id = (emu_reader_id) arg;

    printf("Reader Interrupted by CONTROL-C\n");

    emu_reader_stop(reader_id);
}


static void emu_reader_process(void *arg)
{
    struct emu_thread *thread_descriptor = (struct emu_thread *) arg;
    emu_reader_id reader_id = thread_descriptor->args;
    et_sys_id id = reader_id->id;
    et_id      *etid = (et_id *) id;

    int station_counter = 0;

    EMU_DEBUG(("\nATTACH HANDLER"))

    emu_enable_cancel();
    thread_descriptor->status = EMU_THREAD_ACTIVE;
    while(et_alive(id) && (thread_descriptor->status == EMU_THREAD_ACTIVE))
    {
        int stat_id,status;
        int         num;
        et_station *ps;

        if (reader_id->number_inputs == 0)
            emu_sleep(2);

        //        printf("Max number of inputs %d actual number %d\n",etid->sys->config.nstations/2,reader_id->number_inputs);

        for (num=0; num < reader_id->number_inputs ; num++)
        {
            et_station *ps = etid->grandcentral+reader_id->inputs[num].output_station;

            if (ps->data.status == ET_STATION_ACTIVE)
            {

                et_event   *pe[1000];
                int i, ix, actual = 0;

                stat_id = num;

                //   EMU_DEBUG(("Foundactive Station %s at position %d",ps->name ,stat_id));

                status = et_events_get( etid,reader_id->inputs[num].output_att, pe, ET_SLEEP , NULL,10,&actual);

                if (status != ET_OK)
                {
                    printf("error in et_event_new\n");
                    thread_descriptor->status = EMU_THREAD_ENDED;
                    return;
                }

                for (i =0;i<actual; i++)
                {
                    emu_data_record_ptr record = (emu_data_record_ptr) pe[i]->pdata;

                    /*printf ("reader_process- record number %d from input %d\n",record->record_header.recordNB, record->record_header.rocID);
                    printf ("   length is %d\n", record->record_data.length);
                    for (ix=0;ix<10;ix++)
                {
                        printf("     data[%2d] - %08X\n",ix, record->record_data.data[ix]);
                }
                    printf ("---------------------\n\n");*/
                    // put this code in the builder's input!!
                    //                    pe[i]->length = 0;

                    pe[i]->control[0] =reader_id->inputs[num].input_station;
                    pe[i]->owner = reader_id->gc_att;
                    if (put_cb_data(reader_id->reader_output,(void *) pe[i]) < 0 )
                    {
                        thread_descriptor->status = EMU_THREAD_ENDED;
                        return;
                    }
                }

            }
            else
            {
                thread_descriptor->status = EMU_THREAD_ENDED;
                return;
            }

        }

        //EMU_DEBUG(("Waiting for a station to appear\n" ))
    }

    //emu_thread_cleanup(thread_descriptor);

}
static void emu_reader_simulator(void *arg)
{
    struct emu_thread *thread_descriptor = (struct emu_thread *) arg;
    emu_reader_id reader_id = thread_descriptor->args;
    et_sys_id id = reader_id->id;
    et_id      *etid = (et_id *) id;

    int station_counter = 0;

    EMU_DEBUG(("\nATTACH HANDLER"))

    emu_enable_cancel();
    thread_descriptor->status = EMU_THREAD_ACTIVE;
    while(et_alive(id) && (thread_descriptor->status == EMU_THREAD_ACTIVE))
    {
        int stat_id,status;
        int         num;
        et_station *ps;

        if (reader_id->number_inputs == 0)
            emu_sleep(2);

        //        printf("Max number of inputs %d actual number %d\n",etid->sys->config.nstations/2,reader_id->number_inputs);

        for (num=0; num < reader_id->number_inputs ; num++)
        {
            et_station *ps = etid->grandcentral+reader_id->inputs[num].output_station;

            if (ps->data.status == ET_STATION_ACTIVE)
            {

                et_event   *pe[1000];
                int i, ix, actual = 0;

                stat_id = num;

                //                EMU_DEBUG(("Foundactive Station %s at position %d",ps->name ,stat_id));

                status = et_events_get( etid,reader_id->inputs[num].output_att, pe, ET_SLEEP , NULL,10,&actual);

                if (status != ET_OK)
                {
                    printf("error in et_event_new\n");
                    thread_descriptor->status = EMU_THREAD_ENDED;
                    return;
                }

                for (i =0;i<actual; i++)
                {
                    emu_data_record_ptr record = (emu_data_record_ptr) pe[i]->pdata;

                    printf ("reader simulate - record number %d from input %d\n",record->record_header.recordNB, record->record_header.rocID);
                    printf ("   length is %d\n", record->record_data.length);
                    for (ix=0;ix<10;ix++)
                    {
                        printf("     data[%2d] - %08X\n",ix, record->record_data.data[ix]);
                    }
                    printf ("---------------------\n\n");
                    pe[i]->length = 0;
                    pe[i]->control[0] =reader_id->inputs[num].input_station;
                    pe[i]->owner = reader_id->gc_att;
                }

                //                EMU_DEBUG(("put record back in GC"));

                status = et_events_put(etid,reader_id->gc_att, pe,actual);

                if (status != ET_OK)
                {
                    EMU_DEBUG(("put error"));
                    thread_descriptor->status = EMU_THREAD_ENDED;
                    return;
                }
            }
            else
            {
                thread_descriptor->status = EMU_THREAD_ENDED;
                return;

            }

        }
    }
    //emu_thread_cleanup(thread_descriptor);

}


/*
 * emu emu_reader.c
 * Function reader_initialize station
 *
 *
 */

static void add_input (emu_reader_id reader_id,char *name)
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
            EMU_DEBUG(("put error"));
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

emu_reader_id emu_reader_initialize ( )
{
    emu_reader_id reader_id;
    int           errflg = 0;
    int           i_tmp;

    int           status = 0;
    int           et_verbose = ET_DEBUG_NONE;
    int           deleteFile = 0;
    char *et_filename = emu_config()->emu_name;
    sigset_t      sigblockset;
    et_statconfig sconfig;
    et_sysconfig  config;
    et_stat_id    statid;
    et_sys_id     id;

    if (emu_config()->input_count == 0)
        return NULL;

    reader_id = malloc(sizeof( struct emu_reader));

    bzero(reader_id, sizeof( struct emu_reader));

    /************************************/
    /* default configuration parameters */
    /************************************/
    int nevents = 2000;               /* total number of events */
    int event_size = 40000;            /* size of event in bytes */

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

    //et_system_config_setport(config,emu_configuration->port);
    et_system_config_addmulticast(config, ET_MULTICAST_ADDR);
    et_system_config_addmulticast(config, "239.111.222.0");

    //et_system_config_setcast(config,ET_MULTICAST);

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


    et_system_setdebug(id, ET_DEBUG_INFO);

    reader_id->id = id;

    if (et_station_attach(id, ET_GRANDCENTRAL, &reader_id->gc_att) < 0)
    {
        printf("et_netclient: error in station attach\n");
        exit(1);
    }

    {
        int counter;
        for (counter = 0; counter < emu_config()->input_count; counter++)
        {
            add_input (reader_id,emu_config()->input_names[counter]);

        }
    }
    reader_id->mode = emu_config()->reader_mode;
    esh_add("Reader Control-C handler", interrupt_signal_handler, (void *) reader_id);
    return reader_id;
}

void emu_reader_start(emu_reader_id reader_id)
{
    if (reader_id == NULL)
        printf("No inputs so reader not started\n");

    if (reader_id->mode == EMU_MODE_SIMULATE)
    {
        reader_id->worker_thread = emu_create_thread(1,"Reader Simulator Thread", emu_reader_simulator, (void *) reader_id);
    }
    else
    {
        reader_id->worker_thread = emu_create_thread(1,"Reader Worker Thread", emu_reader_process, (void *) reader_id);
    }


}

void emu_reader_stop(emu_reader_id reader_id)
{
    int status;
    int inputs,counter;

    /* Need to graceully shut down the ET.
     * First detach from the stations. Then remove them.
     * Note: et_station_remove fails if there are still attached processes so we have a loop
     * that retries the remove until success.
     */

    reader_id->keep_going = 0;
    inputs = reader_id->number_inputs;

    for (counter = 0; counter < inputs; counter ++)
    {
        et_station_detach(reader_id->id, reader_id->inputs[counter].input_att);
        et_station_detach(reader_id->id, reader_id->inputs[counter].output_att);
    }
    for (counter = 0; counter < inputs; counter ++)
    {
        while (et_station_remove(reader_id->id, reader_id->inputs[counter].input_station)!= ET_OK)
        {

            EMU_DEBUG(("waiting for producers to detach"))
            emu_sleep(TIME_BEFORE_REMOVE);
        }

        while (et_station_remove(reader_id->id, reader_id->inputs[counter].output_station)!= ET_OK)
        {
            EMU_DEBUG(("waiting for producers to detach"))
            emu_sleep(TIME_BEFORE_REMOVE);
        }
    }

    reader_id->number_inputs = 0;
    et_station_detach(reader_id->id, reader_id->gc_att);
    emu_thread_cleanup(reader_id->worker_thread);
    printf("reader thread cleanup done\n");
    //emu_sleep(2);

    et_system_close(reader_id->id);

    printf("et_system_close has finished\n");
}
