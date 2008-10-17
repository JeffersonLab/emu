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
 *      emu  - gchannel.c
 *
 * 	Implementation of the gchannel API using the ET system
 *
 *----------------------------------------------------------------------------*/
#include <dlfcn.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <stdio.h>

#include "gchannel.h"

extern "C" void gchannel_resolve(gtransport m, char *type) {
    void    *handle;
    char sym_name[100];
    /* open the needed object */
    handle = dlopen(0,RTLD_NOW);

    sprintf(sym_name,"gchannel_create_transport_%s",type);
    /* find the address of function */
    *(void **)(&m->create_transport) = dlsym(handle, sym_name);
    if (m->create_transport == 0) {
        printf("%s : %s not supported\n",dlerror(), sym_name);
    }

    sprintf(sym_name,"gchannel_open_transport_%s",type);
    /* find the address of function */
    *(void **)(&m->open_transport) = dlsym(handle, sym_name);
    if (m->open_transport == 0) {
        printf("%s not supported\n",sym_name);
    }

    sprintf(sym_name,"gchannel_close_transport_%s",type);
    /* find the address of function */
    *(void **)(&m->close_transport) = dlsym(handle, sym_name);
    if (m->close_transport == 0) {
        printf("%s not supported\n",sym_name);
    }

    sprintf(sym_name,"gchannel_create_%s",type);
    /* find the address of function */
    *(void **)(&m->create) = dlsym(handle, sym_name);
    if (m->create == 0) {
        printf("%s not supported\n",sym_name);
    }

    sprintf(sym_name,"gchannel_open_%s",type);
    /* find the address of function */
    *(void **)(&m->open) = dlsym(handle, sym_name);
    if (m->open == 0) {
        printf("%s not supported\n",sym_name);
    }

    sprintf(sym_name,"gchannel_close_%s",type);
    /* find the address of function */
    *(void **)(&m->close) = dlsym(handle, sym_name);
    if (m->close == 0) {
        printf("%s not supported\n",sym_name);
    }

    sprintf(sym_name,"gchannel_send_%s",type);
    /* find the address of function */
    *(void **)(&m->send) = dlsym(handle, sym_name);
    if (m->send == 0) {
        printf("%s not supported\n",sym_name);
    }

    sprintf(sym_name,"gchannel_receive_%s",type);
    /* find the address of function */
    *(void **)(&m->receive) = dlsym(handle, sym_name);
    if (m->receive == 0) {
        printf("%s not supported\n",sym_name);
    }

    sprintf(sym_name,"gchannel_free_%s",type);
    /* find the address of function */
    *(void **)(&m->free) = dlsym(handle, sym_name);
    if (m->free == 0) {
        printf("%s not supported\n",sym_name);
    }
    
    sprintf(sym_name,"gchannel_allocate_%s",type);
    /* find the address of function */
    *(void **)(&m->allocate) = dlsym(handle, sym_name);
    if (m->allocate == 0) {
        printf("%s not supported\n",sym_name);
    }
}

extern "C" void *gchannel_create_transport(JNIEnv *env,char *type, char *transport_name) {

    gtransport m = (gtransport) malloc(sizeof(struct gtransport_str));
    bzero(m, sizeof(struct gtransport_str));

    m->name = strdup(transport_name);
    m->type = strdup(type);
    m->env = env;
    gchannel_resolve(m, type);

    /* invoke function, passing value of integer as a parameter */
    if ((*m->create_transport)(m) == -1) {
        free(m->name);
        free(m->type);
        free(m);
        return NULL;
    }
    return m;
}

extern "C" void *gchannel_open_transport(JNIEnv *env,char *type, char *transport_name) {

    gtransport m = (gtransport) malloc(sizeof(struct gtransport_str));
    bzero(m, sizeof(struct gtransport_str));

    m->name = strdup(transport_name);
    m->type = strdup(type);
    m->env = env;
    gchannel_resolve(m, type);

    /* invoke function, passing value of integer as a parameter */
    if ((*m->open_transport)(m) == -1) {
        free(m->name);
        free(m->type);
        free(m);
        return NULL;
    }
    return m;
}


extern "C" void *gchannel_create(void *transport, char *channel_name) {
    gtransport m = (gtransport) transport;

    gchannel c = (gchannel) malloc(sizeof(struct gchannel_str));
    bzero(c, sizeof(struct gchannel_str));
    char tmp[100];
    sprintf(tmp, "%s:%s",m->name, channel_name);
    c->name = strdup(tmp);
    c->transport = m;
    c->fifo = -1;

    /* invoke function, passing value of integer as a parameter */
    if((*m->create)(c) == -1) {
        free(c->name);
        free(c);
        return NULL;
    }
    return c;
}

extern "C" void *gchannel_open(void *transport, const char *channel_name) {
    gtransport m = (gtransport) transport;

    gchannel c = (gchannel) malloc(sizeof(struct gchannel_str));
    bzero(c, sizeof(struct gchannel_str));

    char tmp[100];
    sprintf(tmp, "%s:%s",m->name, channel_name);
    c->name = strdup(tmp);
    c->transport = m;

    /* invoke function, passing value of integer as a parameter */
    if((*m->open)(c) == -1) {
        free(c->name);
        free(c);
        return NULL;
    }
    return c;
}

extern "C" int gchannel_send(gchannel_hdr *data) {
    gchannel c = data->channel;
    if (c->transport->send != 0)
        return (*c->transport->send)(data); 
    else
        return -1;
}

extern "C" int gchannel_receive(void *channel, gchannel_hdr **data) {
    gchannel c = (gchannel) channel;
    if (c->transport->receive != 0)
        return (*c->transport->receive)(channel, data); 
    else
        return -1;
}

extern "C" int gchannel_allocate(void *channel,gchannel_hdr **data) {
    gchannel c = (gchannel) channel;
    if (c->transport->allocate != 0)
        return (*c->transport->allocate)(channel, data);
    else
        return -1;
}

extern "C" int gchannel_free(gchannel_hdr *data) {
	gchannel_hdr *buf = (gchannel_hdr *) data;
    gchannel c = buf->channel;
    if (c->transport->free != 0)
        return (*c->transport->free)(data);
    else
        return -1;
}

extern "C" int gchannel_close(void *channel) {
    int status;
    gchannel c = (gchannel) channel;

    if (c->transport->close != 0)
        status = (*c->transport->close)(c);
    else
        status = 0;

    free (c->name);
    free (c);
    return status;
}

extern "C" int gchannel_close_transport(void *transport) {
    int status;
    gtransport m = (gtransport) transport;

    if (m->close_transport != 0)
        status = (*m->close_transport)(m);
    else
        status = 0;

    free (m->name);
    free (m->type);
    free (m);
    return status;
}

extern "C" int gchannel_exception(int status, void *transport, char *message) {
    gtransport m = (gtransport) transport;
    if (m->env) {
        jclass newExcCls = m->env->FindClass("org/jlab/coda/support/ex/DataNotFoundException");

        m->env->ThrowNew(newExcCls, message);
    }
    return status;
}
