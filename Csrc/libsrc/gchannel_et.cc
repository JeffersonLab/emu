
/*----------------------------------------------------------------------------
 *  Copyright (c) 2007        Southeastern Universities Research Association,
 *                            Thomas Jefferson National Accelerator Facility
 *
 *    This software was developed under a United States Government license
 *    described in the NOTICE file included as part of this distribution.
 *
 *    Author:  heyes
 *    Created: Jun 20, 2007
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
 *      emu  - gchannel_et.c
 *
 * 	Implementation of the gchannel API using the ET system
 *
 *----------------------------------------------------------------------------*/

#include <stdio.h>
#include "gchannel.h"
#include "gph.h"

#include "et.h"
#include "et_private.h"

#include "jni.h"

typedef  struct et_disc_st *et_desc;

typedef struct et_disc_st {
    et_sys_id     id;
    et_att_id     gc_att;
    int capacity;
    int length;
    char *et_filename;
    int owner;
}
et_desc_ty;

typedef struct et_channel_st *et_channel;

typedef struct et_channel_st {
    int run;
    int active;
    et_stat_id empty_stat;
    et_stat_id full_stat;
    et_att_id empty_att;
    et_att_id full_att;

    pthread_t helper;
}
et_channel_ty;

extern "C" int gchannel_create_transport_et(gtransport m) {
#ifndef CREATE
    et_sysconfig  config;

    et_desc et = (et_desc) malloc(sizeof(et_desc_ty));

    bzero (et,sizeof(et_desc_ty));
    m->hidden = et;

    et->et_filename = m->name;
    et->capacity = 100;
    et->length = 20000;
    et->owner = true;
    if (m->env != NULL) {
        char *value = gchannel_getTransportAttr(m, "pool");

        if (value == NULL )
            return gchannel_exception(-1,(void *) m,"et_start pool attribite missing");

        et->capacity = atoi(value);

        value = gchannel_getTransportAttr(m, "size");

        if (value == NULL )
            return gchannel_exception(-1,(void *) m,"et_start size attribite missing");

        et->length = atoi(value);
    }

    remove(et->et_filename);

    if (et_system_config_init(&config) == ET_ERROR) {
        return gchannel_exception(-1,(void *) m, "et_system_config_init: no more memory");
    }

    et_system_config_addmulticast(config, ET_MULTICAST_ADDR);
    et_system_config_addmulticast(config, "239.111.222.0");

    //et_system_config_setcast(config,ET_MULTICAST);

    /* TODO get this from an attribute - total number of events */
    et_system_config_setevents(config, 2*et->capacity);

    /* TODO get this from an attribute - size of event in bytes */
    et_system_config_setsize(config, et->length);

    /* TODO get this from an attribute - max number of temporary (specially allocated mem) events */
    /* This cannot exceed total # of events                     */
    et_system_config_settemps(config, et->capacity);

    /* limit on # of stations */
    et_system_config_setstations(config, ET_ATTACHMENTS_MAX);

    /* soft limit on # of attachments (hard limit = ET_ATTACHMENTS_MAX) */
    et_system_config_setattachments(config, 200);

    /* soft limit on # of processes (hard limit = ET_PROCESSES_MAX) */
    et_system_config_setprocs(config, ET_PROCESSES_MAX);

    printf("starting ET system %s\n", et->et_filename);

    /* Make sure filename is null-terminated string */
    if (et_system_config_setfile(config, et->et_filename) == ET_ERROR) {
        printf("bad filename argument\n");
        return gchannel_exception(-1,(void *) m, "et_system_config_setfile: bad filename argument");
    }

    /*************************/
    /*    start ET system    */
    /*************************/

    if (et_system_start(&et->id, config) != ET_OK) {
        return gchannel_exception(-1,(void *) m, "et_system_start");

    }

    printf("ET system %s started %08x\n", et->et_filename,et->id);

    //if (getenv("DEBUG") != NULL) {
    et_system_setdebug(et->id, ET_DEBUG_INFO);
    //}

    if (et_station_attach(et->id, ET_GRANDCENTRAL, &et->gc_att) < 0) {
        return gchannel_exception(-1,(void *) m, "et_netclient: error in station attach\n");
    }

#else

    et_openconfig  openconfig;
    struct timespec time_to_wait;
    time_to_wait.tv_nsec = 0;
    time_to_wait.tv_sec = 4;

    et_desc et = (et_desc) malloc(sizeof(et_desc_ty));

    bzero (et,sizeof(et_desc_ty));
    m->hidden = et;

    et->et_filename = m->name;
    et->capacity = 100;
    et->length = 20000;

    if (m->env != NULL) {
        char *value = gchannel_getTransportAttr(m, "pool");

        if (value == NULL )
            return gchannel_exception(-1,(void *) m,"et_start pool attribite missing");

        et->capacity = atoi(value);

        value = gchannel_getTransportAttr(m, "size");

        if (value == NULL )
            return gchannel_exception(-1,(void *) m,"et_start size attribite missing");

        et->length = atoi(value);
    }

    /************************************/
    /* default configuration parameters */
    /************************************/

    et_open_config_init(&openconfig);

    et_open_config_setcast(openconfig, ET_MULTICAST);
    et_open_config_addmulticast(openconfig, ET_MULTICAST_ADDR);
    et_open_config_addmulticast(openconfig, "239.111.222.0");

    //et_open_config_setmode(openconfig, ET_HOST_AS_REMOTE);
    et_open_config_setmode(openconfig, ET_HOST_AS_LOCAL);

    et_open_config_setwait(openconfig,ET_OPEN_WAIT);
    et_open_config_settimeout(openconfig,time_to_wait);
    //et_open_config_setpolicy(openconfig,ET_POLICY_FIRST);

    printf("try to open ET system %s\n",et->et_filename);
    if (et_open(&et->id, et->et_filename, openconfig) != ET_OK) {
        return gchannel_exception(-1,(void *) m, "et_open");
    }

    //if (getenv("DEBUG") != NULL) {
    et_system_setdebug(et->id, ET_DEBUG_INFO);
    //}

    if (et_station_attach(et->id, ET_GRANDCENTRAL, &et->gc_att) < 0) {
        return gchannel_exception(-1,(void *) m, "et_netclient: error in station attach\n");
    }
#endif
    return 0;
}

extern "C" void *gchannel_read_helper_et(void *arg) {
    gchannel c = (gchannel) arg;
    et_desc et = (et_desc) c->transport->hidden;
    et_channel et_chan = (et_channel) c->hidden;
    et_sys_id id = et->id;
    et_id      *etid = (et_id *) id;
    struct timespec wait;

    wait.tv_nsec = 0;
    wait.tv_sec = 1;

    et_chan->active = TRUE;
    while(et_alive(id) && (et_chan->run)) {
    	pthread_testcancel();
        int stat_id,status;
        int         num;

        et_station *ps = etid->grandcentral+et_chan->full_stat;

        et_event   *pe[et->capacity];
        int i, actual = 0;

        stat_id = num;

        printf("et_events_get %d\n",et->capacity);

        status = et_events_get( etid,et_chan->full_att, pe, ET_TIMED , &wait,et->capacity,&actual);
        printf("et_events_get done and got %d\n", actual);
        if ((status != ET_OK) && (status != ET_ERROR_TIMEOUT)) {
            printf("et_producer: et_events_get error\n");
            // instead of breaking do something smart here!!
            et_chan->run = FALSE;
            break;
        }

        if (status == ET_ERROR_TIMEOUT)
            continue;

        for (i =0;i<actual; i++) {
            void *data;
            size_t length;

            c->record_count++;
            c->word_count += (pe[i]->length>>2);

            printf ("reader_process- record number %lld\n",c->record_count);

            et_event_getdata (pe[i], &data);
            et_event_getlength (pe[i], &length);

            printf("length is %d\n",length);
            printf("data is %p\n",data);

            printf("Put on %08x\n",c->fifo);

            void *data_out = (void *) malloc(pe[i]->length);
            printf("bcopy %p to %p\n", pe[i]->data,data_out);
            bcopy(data, data_out,length);

            do {
                // gdf_get will return a status of ETIMEDOUT after 0.25 seconds.
                status = gdf_put(c->fifo, (long int*)data_out);

                if (et_chan->run == FALSE) {

                    goto et_channel_end;
                }
            } while (status == ETIMEDOUT);

            pe[i]->length = 0;

        }
        //printf("et_events_put %d\n",actual);
        status = et_events_put(et->id,et_chan->full_att, pe, actual);
        //printf("et_events_put done %d\n",actual);
        if (status != ET_OK) {
            printf("put error\n");
            et_chan->run = FALSE;
            break;
        }


    }
et_channel_end:
    et_chan->run = FALSE;
    et_chan->active = FALSE;

    pthread_exit(NULL);
    return NULL;
}

extern "C" int gchannel_create_et(gchannel c) {
    gtransport m = c->transport;
    et_statconfig	 sconfig;
    int status;
    et_stat_id	 my_stat;
    char tmp[100];
    int selection[4] = {0,-1,-1,-1};

    et_desc et = (et_desc) c->transport->hidden;
    et_channel et_chan = (et_channel) malloc(sizeof(et_channel_ty));
    bzero(et_chan, sizeof(et_channel_ty));

    c->hidden = et_chan;
    et_chan->run = TRUE;

    /* Since both the consumer and the producer  need to attach to the input we use
     * ET_STATION_USER_MULTI
     */

    et_station_config_init(&sconfig);
    et_station_config_setuser(sconfig, ET_STATION_USER_MULTI);
    et_station_config_setrestore(sconfig, ET_STATION_RESTORE_OUT);
    et_station_config_setprescale(sconfig, 1);
    et_station_config_setcue(sconfig, et->capacity);
    et_station_config_setselect(sconfig, ET_STATION_SELECT_MATCH);
    et_station_config_setblock(sconfig, ET_STATION_BLOCKING);

    et_chan->empty_stat = my_stat;

    et_station_config_setuser(sconfig, ET_STATION_USER_MULTI);

    sprintf(tmp,"%s_full",c->name);
    if ((status = et_station_create(et->id, &my_stat, tmp, sconfig)) < ET_OK) {
        if (status != ET_ERROR_EXISTS)
            goto station_error;
    }

    selection[0] = my_stat;

    et_station_setselectwords(et->id, my_stat,selection);

    et_chan->full_stat = my_stat;

    /* Now attach to the station that we just created.
    */

    if (et_station_attach(et->id, et_chan->full_stat, &et_chan->full_att) < 0) {
        return gchannel_exception(-1,(void *) m, "et_netclient: error in station attach\n");
    }

    et_station_setcue(et->id,et_chan->full_stat,et->capacity);

    sprintf(tmp,"%s channel output",c->name);

    c->fifo = gdf_new_sized(tmp, et->capacity);

    sprintf(tmp,"%s channel helper",c->name);
    pthread_create(&et_chan->helper, NULL, gchannel_read_helper_et, (void *) c);

    return 0;

station_error:

    if (status == ET_ERROR_EXISTS) {
        /* my_stat contains pointer to existing station */;
        return gchannel_exception(-1,(void *) m, "et_netclient: station already exists\n");
    } else if (status == ET_ERROR_TOOMANY) {
        return gchannel_exception(-1,(void *) m, "et_netclient: too many stations created\n");
    } else if (status == ET_ERROR_REMOTE) {
        return gchannel_exception(-1,(void *) m, "et_netclient: memory or improper arg problems\n");
    } else if (status == ET_ERROR_READ) {
        return gchannel_exception(-1,(void *) m, "et_netclient: network reading problem\n");
    } else if (status == ET_ERROR_WRITE) {
        return gchannel_exception(-1,(void *) m, "et_netclient: network writing problem\n");
    } else {
        return gchannel_exception(-1,(void *) m, "et_netclient: error in station creation\n");
    }

    return 0;
}

extern "C" int gchannel_open_transport_et(gtransport m) {

    et_openconfig  openconfig;
    struct timespec time_to_wait;
    time_to_wait.tv_nsec = 0;
    time_to_wait.tv_sec = 4;

    et_desc et = (et_desc) malloc(sizeof(et_desc_ty));

    bzero (et,sizeof(et_desc_ty));
    m->hidden = et;

    et->et_filename = m->name;
    et->owner = false;

    if (m->env != NULL) {
        char *value = gchannel_getTransportAttr(m, "pool");

        et->capacity = atoi(value);

        value = gchannel_getTransportAttr(m, "size");

        et->length = atoi(value);
    }
    /* This thread takes care of the connection to ET. We are a process remote from the ET so
     * everything happens over  the network. It's done as a thread because I don't want the open
     * to block the whole program.
    */

    et_open_config_init(&openconfig);

    et_open_config_setcast(openconfig, ET_BROADANDMULTICAST);
    et_open_config_addmulticast(openconfig, ET_MULTICAST_ADDR);
    et_open_config_addmulticast(openconfig, "239.111.222.0");
    et_open_config_setmode(openconfig, ET_HOST_AS_REMOTE);

    et_open_config_setwait(openconfig,ET_OPEN_WAIT);
    et_open_config_settimeout(openconfig,time_to_wait);
    et_open_config_setpolicy(openconfig,ET_POLICY_FIRST);

    if (et_open(&et->id, et->et_filename, openconfig) != ET_OK) {
        return gchannel_exception(-1,(void *) m, "et_open: cannot open ET system\n");
    }

    et_open_config_destroy(openconfig);

    et_system_setdebug(et->id,ET_DEBUG_INFO);

    if (et_station_attach(et->id, ET_GRANDCENTRAL, &et->gc_att) < 0) {
        return gchannel_exception(-1,(void *) m, "et_station_attach: error in station attach\n");
    }
    return 0;
}

extern "C" void *gchannel_send_helper_et(void *arg) {
    gchannel c = (gchannel) arg;
    et_desc et = (et_desc) c->transport->hidden;
    et_channel et_chan = (et_channel) c->hidden;

    int ix, count, num_et_new, num_filled, status = 0;
    et_event   *pe[1000];
    void *data[1000];

    struct timespec wait;

    wait.tv_nsec = 0;
    wait.tv_sec = 1;

    et_chan->active = TRUE;
    while ( et_chan->run ) {
		pthread_testcancel();
        count = et->capacity;

        // Loop to get some buffers.
        do {
            // printf ("gchannel_send_helper_et : asking for %d\n", count);
            status = et_events_new(et->id,et->gc_att,pe, ET_TIMED | ET_NOALLOC, &wait,et->length, count,&num_et_new);
            if ((status != ET_OK) && (status != ET_ERROR_TIMEOUT)) {
                printf("et_producer: et_events_new error\n");
                et_chan->run = FALSE;
                goto gchannel_send_helper_cleanup;
            }
            // printf("Status is %d got %d\n",status, num_et_new);
        } while ((status != ET_OK) && (et_chan->run));

        // At this point we are holding a number of buffers equal to "num_et_new"
        num_filled = 0;
        do {
            // printf("get from in fifo\n");
            long *in_data = NULL;
            int status;

            do {
                // gdf_get will return a status of ETIMEDOUT after 0.25 seconds.
                status = gdf_get(c->fifo, &in_data);

                // printf("got\n");
                if (et_chan->run == FALSE) {
                    goto gchannel_send_helper_cleanup;
                }
            } while (status == ETIMEDOUT);

            data[num_filled] = pe[num_filled]->pdata = in_data;

            // WARNING : ET event length is in BYTES not words, record length is 32-bit word count
            //
            //printf("union %d header %d\n", sizeof(emu_data_record_union), (record->record_header.length<<2));

            pe[num_filled]->length = *((long *) in_data)<<2;

            // When we put into GC the events will move to the "full" station for this channel
            pe[num_filled]->control[0] = et_chan->full_stat;

            num_filled++;
        } while (--num_et_new);

        // printf("sending %d\n",actual);

        do {
            status = et_events_put(et->id,et->gc_att, pe,num_filled);
            if ((status != ET_OK) && (status != ET_ERROR_TIMEOUT)) {
                printf("et_producer: et_events_new error\n");
                // instead of breaking do something smart here!!
                et_chan->run = FALSE;
                goto gchannel_send_helper_cleanup;
            }
        } while ((status != ET_OK) && (et_chan->run));

        // if the put failed or not we still need to call free.
        for (ix=0;ix<num_filled;ix++) {
            free(data[ix]);
        }

    }

gchannel_send_helper_cleanup:

    et_chan->run = FALSE;
    et_chan->active = FALSE;

    pthread_exit(NULL);

}

extern "C" int gchannel_open_et(gchannel c) {
    et_desc et = (et_desc) c->transport->hidden;
    gtransport m = c->transport;
    et_channel et_chan = (et_channel) malloc(sizeof(et_channel_ty));
    bzero(et_chan, sizeof(et_channel_ty));
    c->hidden = et_chan;
    et_chan->run = TRUE;
    char *tmp = (char *) malloc(strlen(c->name) + 8);


    sprintf(tmp,"%s_full",c->name);

    if (et_station_name_to_id(et->id,&et_chan->full_stat,tmp) < 0) {
        char *tmp2 = (char *) malloc(strlen(c->name) +100);
        sprintf(tmp2, "error finding station %s", tmp);
        free(tmp);
        free(tmp2);
        gchannel_exception(-1,(void *) m, tmp2);
        return -1;
    }

    et_station_getcue(et->id,et_chan->full_stat,&et->capacity);

    sprintf(tmp,"%s channel fifo",c->name);
    c->fifo = gdf_new_sized(tmp, et->capacity);

    // open is used by writers so set up a thread to get records from the FIFO and put them to the ET.
    sprintf(tmp,"%s channel helper",c->name);
    pthread_create(&et_chan->helper, NULL, gchannel_send_helper_et, (void *) c);
    free(tmp);
    return 0;
}

extern "C" int gchannel_send_et(gchannel channel,int32_t *data) {
    return gdf_put(channel->fifo,(long *) data);
}

extern "C" int gchannel_receive_et(gchannel channel, int32_t **data) {
    //printf("waiting on fifo for data\n");
    int status = gdf_get(channel->fifo, (long **)data);
    //printf("gdf_get returns %d %p \n",status,*data);
    return status;
}

extern "C" int gchannel_close_transport_et(gtransport m) {
    printf("GTRANSPORT_CLOSE_ET\n");
    et_desc et = (et_desc) m->hidden;
    if (et->owner)
        et_system_close(et->id);
    else
        et_forcedclose(et->id);
    return 0;
}

extern "C" int gchannel_close_et(gchannel c) {
    printf("GCHANNEL_CLOSE_ET\n");
    et_desc et = (et_desc) c->transport->hidden;
    et_channel et_chan = (et_channel) c->hidden;
    et_chan->run = FALSE;

    et_wakeup_attachment(et->id,et->gc_att);

    pthread_join(et_chan->helper, NULL);

    gdf_free(c->fifo);
    printf("\tet_station_detach\n");
    et_station_detach(et->id,et_chan->full_att);
    //printf("\tet_station_remove\n");
    //et_station_remove (et->id, et_chan->full_stat);
    printf("GCHANNEL_CLOSE_ET- DONE\n");
    return 0;
}
