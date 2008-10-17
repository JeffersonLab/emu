/*----------------------------------------------------------------------------
 *  Copyright (c) 2007        Southeastern Universities Research Association,
 *                            Thomas Jefferson National Accelerator Facility
 *
 *    This software was developed under a United States Government license
 *    described in the NOTICE file included as part of this distribution.
 *
 *    Author:  heyes
 *    Created: 7 Mar 2007
 *    Modification date : $Date: 2007-06-05 10:03:31 -0400 (Tue, 05 Jun 2007) $
 *    Revision : $Revision: 6174 $
 *    URL : $HeadURL: file:///daqfs/source/svnroot/coda/3.0/emu/emu_int_fifo.h $
 *
 *             heyes@jlab.org                    Jefferson Lab, MS-12H
 *             Phone: (757) 269-7030             12000 Jefferson Ave.
 *             Fax:   (757) 269-5800             Newport News, VA 23606
 *
 *----------------------------------------------------------------------------
 *
 * Description:
 *      emu  - emu_int_fifo.h
 *
 *----------------------------------------------------------------------------*/

#ifndef _GDF_
#define _GDF_
#include <pthread.h>
#include <errno.h>

#define QSIZE                           10
// TODO Modify calls to return status
typedef struct gdf {
	int active;
    char *name;

    int  wait_get;
    int  wait_put;
    int start_idx;                /* start of valid data */
    int num_full;                 /* # of full locations */

    pthread_mutex_t buf_lock;     /* lock the structure */
    pthread_cond_t notfull;       /* full -> not full condition */
    pthread_cond_t notempty;      /* empty -> notempty condition */
    pthread_cond_t empty;         /* empty -> notempty condition */
    int qsize;
    long **data;            /* Circular buffer of pointers */
}
gdf_struc;

#ifdef __cplusplus
extern "C"  void gdf_sys_init(int max_fifos) ;
extern "C"  int gdf_new(char *name);
extern "C"  int gdf_new_sized(char *name, int size);
extern "C"  void  gdf_free(int fifo);
extern "C"  int   gdf_put(int fifo, long *data);
extern "C"  int   gdf_count(int fifo);
extern "C"  int gdf_get(int fifo, long **data);
extern "C"  char *gdf_name(int fifo);
extern "C"  int gdf_list(void *arg);
#else
extern void gdf_sys_init(int max_fifos); 
extern int gdf_new(char *name);
extern int gdf_new_sized(char *name, int size);
extern void  gdf_free(int fifo);
extern int   gdf_put(int fifo, long *data);
extern int   gdf_count(int fifo);
extern int gdf_get(int fifo, long **data);
extern char *gdf_name(int fifo);
extern int gdf_list(void *arg);
#endif
#endif /*_GDF_*/

