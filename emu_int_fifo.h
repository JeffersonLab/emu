/*----------------------------------------------------------------------------
 *  Copyright (c) 2007        Southeastern Universities Research Association,
 *                            Thomas Jefferson National Accelerator Facility
 *
 *    This software was developed under a United States Government license
 *    described in the NOTICE file included as part of this distribution.
 *
 *    Author:  heyes
 *    Created: 7 Mar 2007
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
 *      emu  - emu_int_fifo.h
 *
 *----------------------------------------------------------------------------*/

#ifndef EMU_INT_FIFO_H_
#define EMU_INT_FIFO_H_

#define QSIZE                           500

typedef struct cbt {
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
} circ_buf_t;

extern circ_buf_t *new_cb(char *name);
extern void  delete_cb(circ_buf_t *cbp);
extern int   put_cb_data(circ_buf_t *cbp, void *data);
extern int   get_cb_count(circ_buf_t *cbp);
extern void *get_cb_data(circ_buf_t *cbp);
extern char *get_cb_name(circ_buf_t *cbp);

#endif /*EMU_INT_FIFO_H_*/

