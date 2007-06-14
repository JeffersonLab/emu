/*----------------------------------------------------------------------------
 *  Copyright (c) 2007        Southeastern Universities Research Association,
 *                            Thomas Jefferson National Accelerator Facility
 *
 *    This software was developed under a United States Government license
 *    described in the NOTICE file included as part of this distribution.
 *
 *    Author:  heyes
 *    Created: May 17, 2007
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
 *      emu  - emu_linked_list.h
 *
 *----------------------------------------------------------------------------*/

#ifndef GENERIC_LINKED_LIST_H_
#define GENERIC_LINKED_LIST_H_
#define FALSE 0
#include <pthread.h>

typedef struct gll_el_str *gll_el;

typedef struct gll_el_str
{
	struct gll_li_str *ell_li;
    void  *payload;
    gll_el next;
    gll_el previous;
}
gll_el_ty;

typedef struct gll_li_str *gll_li;

typedef struct gll_li_str
{
    char *name;
    pthread_mutex_t lock;     /* lock the structure */
    gll_el first;
    gll_el last;
}
gll_list_type;


gll_li gll_create_li(char *name);
gll_el gll_add_el (gll_li l, void *data);
//gll_el ell_remove (gll_li lp,void *data);
gll_el gll_remove_el (gll_el el);
gll_el gll_insert_el (gll_el el,void *data);
gll_el gll_get_first(gll_li l);
gll_el gll_get_last(gll_li l);
int gll_get_count(gll_li l);
gll_el gll_get_next(gll_el el);
gll_el gll_get_prev(gll_el el);
gll_el gll_find_el (gll_li li,void *data);
void *gll_get_data(gll_el el);

void gll_print_li (gll_li l);
int gll_list();
void gll_clear_li (gll_li l);

#endif /*GENERIC_LINKED_LIST_H_*/
