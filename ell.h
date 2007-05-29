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

#ifndef EMU_LINKED_LIST_H_
#define EMU_LINKED_LIST_H_
#define FALSE 0
#include <pthread.h>

typedef struct ell_el_str *ell_el;

typedef struct ell_el_str
{
    void  *payload;
    ell_el next;
    ell_el previous;
}
ell_el_ty;

typedef struct ell_li_str *ell_li;

typedef struct ell_li_str
{
    char *name;
    pthread_mutex_t lock;     /* lock the structure */
    ell_el first;
    ell_el last;
}
emu_list_type;


ell_li ell_create_li(char *name);
ell_el ell_add_el (ell_li l, void *data);
ell_el ell_del_el (ell_li lp,void *data);
void ell_print_li (ell_li l);
void ell_clear_li (ell_li l);

#endif /*EMU_LINKED_LIST_H_*/
