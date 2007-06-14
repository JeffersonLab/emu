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
#define QSIZE                           500

typedef struct gdf {
  char *name;
  char *parent;
  int  wait_get;
  int  wait_put;
  pthread_mutex_t buf_lock;     /* lock the structure */
  int start_idx;                /* start of valid data */
  int num_full;                 /* # of full locations */
  int deleting;                 /* are we in a delete somewhere? */
  pthread_cond_t notfull;       /* full -> not full condition */
  pthread_cond_t notempty;      /* empty -> notempty condition */
  pthread_cond_t empty;         /* empty -> notempty condition */
  int qsize;
  void **data;            /* Circular buffer of pointers */
} gdf_struc;

extern gdf_struc *gdf_new(char *name);
extern gdf_struc *gdf_new_sized(char *name, int size);
extern void  gdf_delete(gdf_struc *cbp);
extern int   put_cb_data(gdf_struc *cbp, void *data);
extern int   gdf_count(gdf_struc *cbp);
extern void *gdf_get(gdf_struc *cbp);
extern char *gdf_name(gdf_struc *cbp);

#endif /*_GDF_*/

