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
 *      emu  - emu_linked_list.c
 *
 *----------------------------------------------------------------------------*/

/*							     */
/* queue.c 						     */
/* Demo of dynamic data structures in C                      */

#include <stdlib.h>
#include <stdio.h>
#include "ell.h"
#include "strings.h"

//#define TEST_MAIN
#ifdef TEST_MAIN
void Menu (int *choice)
{
    char    local;

    printf ("\nEnter\t1 to add item,\n\t2 to remove item\n\t3 to print queue\n\t4 to quit\n");
    do
    {
        local = getchar ();
        if ((isdigit (local) == FALSE) && (local != '\n'))
        {
            printf ("\nyou must enter an integer.\n");
            printf ("Enter 1 to add, 2 to remove, 3 to print, 4 to quit\n");
        }
    }
    while (isdigit ((unsigned char) local) == FALSE);
    *choice = (int) local - '0';
}

main ()
{
    ell_el listmember;
    ell_li listpointer;

    int     data,
    choice;

    listpointer = ell_create_li("TEST list");
    if (listpointer == NULL)
    {
        exit(-1);
    }
    do
    {
        Menu (&choice);
        switch (choice)
        {
        case 1:
            printf ("Enter data item value to add  ");
            scanf ("%d", &data);
            ell_add_el (listpointer, (void *) data);
            break;
        case 2:
            if (listpointer == NULL)
                printf ("Queue empty!\n");
            else
            {
                printf ("Enter data item value to remove  ");
                scanf ("%d", &data);
                listmember = ell_remove_el (listpointer, (void *) data);
                if (listmember == NULL)
                {
                    printf("item not found in list\n");
                }
            }
            break;
        case 3:
            ell_print_li (listpointer);
            break;

        case 4:
            break;

        default:
            printf ("Invalid menu choice - try again\n");
            break;
        }
    }
    while (choice != 4);
    ell_clear_li (listpointer);
}				/* main */
#endif

ell_li ell_create_li(char *name)
{

    ell_li lp = NULL;
    if (name != NULL)
    {
        lp = (ell_li) malloc(sizeof(emu_list_type));
        bzero ((void *) lp, sizeof(emu_list_type));
        lp->name = strdup(name);
        pthread_mutex_init(&lp->lock, NULL);
    }
    else
        printf("list must have a name\n");
    return lp;
}

ell_el ell_add_el (ell_li lp, void * data)
{

    ell_el le = (ell_el) malloc(sizeof(ell_el_ty));
    bzero((void *) le, sizeof(ell_el_ty));

    le->ell_li = lp;

    le->payload = data;
    pthread_mutex_lock(&lp->lock);

    if (lp->last != NULL)
    {
        // if last is not NULL then we tag on to end of list
        le->previous = lp->last;
        le -> next = NULL;

        lp->last->next = le;

        lp->last = le;
    }
    else
    {
        // if last is NULL then first is also NULL and the list is empty
        lp -> first = le;
        lp->last = le;
        le->previous = NULL;
        le->next = NULL;
    }
    pthread_mutex_unlock(&lp->lock);
    return le;
}

ell_el ell_insert_el (ell_el elem, void * data)
{

    pthread_mutex_lock(&elem->ell_li->lock);

    ell_el le = (ell_el) malloc(sizeof(ell_el_ty));
    bzero((void *) le, sizeof(ell_el_ty));
    le->ell_li = elem->ell_li;

    le->payload = data;

    le->next = elem->next;
    elem->next = le;
    le->previous = elem;
    pthread_mutex_unlock(&elem->ell_li->lock);
    return le;
}

ell_el ell_remove_el (ell_el el)
{
    ell_el le;
	ell_li lp = el->ell_li;

    pthread_mutex_lock(&lp->lock);

    le = lp->first;

    if (le == NULL)
    {
        printf("WARNING: searching for %08x when list %s was empty!\n", el, lp->name);
        pthread_mutex_unlock(&lp->lock);
        return NULL;
    }
    // search list for item to remove
    while(le != NULL)
    {
        if (le == el)
        {
            // special cases first

            if ( (le == lp->first) && (le == lp->last))
            {
                // Only item in list?
                printf("only item on list\n");
                lp->first = NULL;
                lp->last = NULL;
            }
            else if (le == lp->first)
            {
                // start of list
                printf("first item on list\n");
                lp->first = le->next;
                lp->first->previous = NULL;
            }
            else if (le == lp->last)
            {
                //end of list
                printf("last item on list\n");
                lp->last = le->previous;
                lp->last->next = NULL;
            }
            else
            {
                // somewhere in the middle
                printf("somewhere in the middle\n");
                le->previous->next = le->next;
                le->next->previous = le->previous;
            }
            free(le);
            printf ("Element %08x removed from %s\n", el, lp->name);
            goto done_remove;
        }
        // not found yet so keep looking
        le = le->next;
    }
    le = NULL;
    printf("WARNING: %08x not found in list %s\n", el,lp->name);
done_remove:
    pthread_mutex_unlock(&lp->lock);
    return le;
}

ell_el ell_get_first(ell_li lp)
{
	return lp->first;
}

ell_el ell_get_last(ell_li lp)
{
	return lp->last;
}

ell_el ell_get_next(ell_el el)
{
	return el->next;
}

ell_el ell_get_prev(ell_el el)
{
	return el->previous;
}

void *ell_get_data(ell_el el)
{
	return el->payload;
}

int ell_get_count(ell_li lp)
{
	int counter = 0;
	ell_el le = lp->first;
	while (le != NULL) {
		counter++;
		le = le->next;
	}
	return counter;
}

void ell_print_li (ell_li lp)
{
    pthread_mutex_lock(&lp->lock);
    ell_el le;

    printf ("Dump of list called %s\n", lp->name);
    printf ("First element %08x\n", lp->first);
    printf ("Last element %08x\n", lp->last);

    if (lp->first == NULL)
        printf ("\tlist is empty!\n");
    else
    {
        le = lp->first;
        while (le != NULL)
        {
            printf ("\t%08x,", le -> payload);
            le = le -> next;
        }
    }
    printf ("\n");
    pthread_mutex_unlock(&lp->lock);
}

void ell_clear_li (ell_li lp)
{
    ell_el le,tmp;
    pthread_mutex_lock(&lp->lock);

    le = lp->first;
    while (le != NULL)
    {
        tmp = le;
        le = le -> next;
        free(tmp);
    }
    pthread_mutex_unlock(&lp->lock);
}
