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
 *      emu  - gplumb.h
 *
 *----------------------------------------------------------------------------*/

#ifndef GCHANNEL_H_
#define GCHANNEL_H_

#ifndef TRUE
#define FALSE 0
#define TRUE  1
#endif
#include <stdlib.h>
#include <stdio.h>
#include <stdarg.h>
#include <strings.h>
#include "sys/errno.h"
#include "jni.h"
#include "gll.h"
#include "gdf.h"

typedef struct gchannel_str *gchannel;

typedef struct gchannel_hdr_str {
	gchannel channel;
	int32_t length;
	int32_t data[0];
} gchannel_hdr;

typedef struct gtransport_str *gtransport;

typedef struct gtransport_str {
	JNIEnv *env;
    char *type;
    char *name;
    char *transport_name;
    gll_li channels;
    char *host;
    void *hidden;

    int (*create_transport) (void *transport);
    int (*open_transport) (void *transport);
    int (*create) (void *channel);
    int (*open) (void *channel);

    int (*close_transport) (void *transport);
    int (*close) (void *channel);

    int (*send)   (gchannel_hdr *data);
    int (*receive)(void *channel, gchannel_hdr **data);
    int (*allocate)(void *channel, gchannel_hdr **data);
	int (*free)   (gchannel_hdr *data); 
}
gtransport_ty;

typedef struct gchannel_str {
    char *type;
    gtransport transport;
    char *name;
    long long record_count;
    long long word_count;
    void *hidden;
    int fifo;
    int fifo_aux;
}
gchannel_ty;

#define GCHANNEL_INPUT(c) (((gchannel) c)->in);
#define GCHANNEL_OUTPUT(c) (((gchannel) c)->out);
#ifdef __cplusplus
extern "C" void gchannel_resolve(gtransport m, char *type);
extern "C" void *gchannel_create_transport(JNIEnv *env,char *type, char *name);
extern "C" void *gchannel_open_transport(JNIEnv *env,char *type, char *name);
extern "C" void *gchannel_create(void *transport,char *name);
extern "C" void *gchannel_open(void *transport, const char *name);
extern "C" int   gchannel_send(gchannel_hdr *data);
extern "C" int   gchannel_receive(void *channel, gchannel_hdr **data);
extern "C" int   gchannel_allocate(void *channel, gchannel_hdr **data);
extern "C" int   gchannel_free(gchannel_hdr *data);
extern "C" int   gchannel_close(void *channel);
extern "C" int   gchannel_close_transport(void *transport);
extern "C" char *gchannel_getTransportAttr(void *transport, char *name);
extern "C" int gchannel_exception(int status, void *transport, char *message);
#else
extern void gchannel_resolve(gtransport m, char *type);
extern void *gchannel_create_transport(JNIEnv *env,char *type, char *name);
extern void *gchannel_open_transport(JNIEnv *env,char *type, char *name);
extern void *gchannel_create(void *transport,char *name);
extern void *gchannel_open(void *transport, char *name);
extern int   gchannel_send(gchannel_hdr *data);
extern int   gchannel_receive(void *channel, gchannel_hdr **data);
extern int   gchannel_allocate(void *channel, gchannel_hdr **data);
extern int   gchannel_free(gchannel_hdr *data);
extern int   gchannel_close(void *channel);
extern int   gchannel_close_transport(void *transport);
extern char *gchannel_getTransportAttr(void *transport, char *name);
extern int gchannel_exception(int status, void *transport, char *message);
#endif
#endif /*GCHANNEL_H_*/
