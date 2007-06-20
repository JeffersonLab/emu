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
/* Demo of dynamic data structures in C           */

#include <stdlib.h>
#include <stdio.h>
#include "gll.h"
#include "strings.h"
#define DASH  "---------------------------------"

//#define TEST_MAIN
#ifdef TEST_MAIN
void Menu (int *choice) {
    char    local;

    printf ("\nEnter\t1 to add item,\n\t2 to remove item\n\t3 to print queue\n\t4 to quit\n");
    do {
        local = getchar ();
        if ((isdigit (local) == FALSE) && (local != '\n')) {
            printf ("\nyou must enter an integer.\n");
            printf ("Enter 1 to add, 2 to remove, 3 to print, 4 to quit\n");
        }
    } while (isdigit ((unsigned char) local) == FALSE);
    *choice = (int) local - '0';
}

main () {
    gll_el listmember;
    gll_li listpointer;

    int     data,
    choice;

    listpointer = gll_create_li("TEST list");
    if (listpointer == NULL) {
        exit(-1);
    }
    do {
        Menu (&choice);
        switch (choice) {
        case 1:
            printf ("Enter data item value to add  ");
            scanf ("%d", &data);
            gll_add_el (listpointer, (void *) data);
            break;
        case 2:
            if (listpointer == NULL)
                printf ("Queue empty!\n");
            else {
                printf ("Enter data item value to remove  ");
                scanf ("%d", &data);
                listmember = gll_remove_el (listpointer, (void *) data);
                if (listmember == NULL) {
                    printf("item not found in list\n");
                }
            }
            break;
        case 3:
            gll_print_li (listpointer);
            break;

        case 4:
            break;

        default:
            printf ("Invalid menu choice - try again\n");
            break;
        }
    } while (choice != 4);
    gll_clear_li (listpointer);
}				/* main */
#endif
static gll_li gll_li_li = NULL;

gll_st gll_create_st() {
    gll_st st;

    st = (gll_st) malloc(sizeof(gll_stack_type));
    bzero ((void *) st, sizeof(gll_stack_type));

    pthread_mutex_init(&st->lock, NULL);
    return st;
}

void *gll_pop(gll_st st) {
    if (st == NULL)
        return;

    gll_el top = st->top;
    void *data;

    if (top == NULL)
        return NULL;

    pthread_mutex_lock(&st->lock);
	st->depth--;
    st->top = top->next;
    data = top->payload;
    free(top);

    pthread_mutex_unlock(&st->lock);
    return data;
}

void gll_push(gll_st st,void *data) {
    if (st == NULL)
        return;

    gll_el le = (gll_el) malloc(sizeof(gll_el_ty));
    bzero((void *) le, sizeof(gll_el_ty));

    le->payload = data;

    pthread_mutex_lock(&st->lock);

    gll_el top = st->top;
	st->depth++;
    st->top = le;

    le->next = top;

    pthread_mutex_unlock(&st->lock);

}

gll_li gll_create_li(char *name) {
    gll_li lp = NULL;

    if (gll_li_li == NULL) {
        gll_li_li = (gll_li) malloc(sizeof(gll_list_type));
        bzero ((void *) gll_li_li, sizeof(gll_list_type));
        gll_li_li->name = strdup("Master list of lists");
        pthread_mutex_init(&gll_li_li->lock, NULL);
    }

    if (name != NULL) {
        lp = (gll_li) malloc(sizeof(gll_list_type));
        bzero ((void *) lp, sizeof(gll_list_type));
        lp->name = strdup(name);
        pthread_mutex_init(&lp->lock, NULL);
        gll_add_el(gll_li_li, lp);
    } else
        printf("list must have a name\n");
    return lp;
}

gll_el gll_add_el (gll_li lp, void * data) {
    if (lp == NULL || data == NULL)
        return NULL;

    gll_el le = (gll_el) malloc(sizeof(gll_el_ty));
    bzero((void *) le, sizeof(gll_el_ty));

    le->ell_li = lp;

    le->payload = data;
    pthread_mutex_lock(&lp->lock);

    if (lp->last != NULL) {
        // if last is not NULL then we tag on to end of list
        le->previous = lp->last;
        le -> next = NULL;

        lp->last->next = le;

        lp->last = le;
    } else {
        // if last is NULL then first is also NULL and the list is empty
        lp -> first = le;
        lp->last = le;
        le->previous = NULL;
        le->next = NULL;
    }
    pthread_mutex_unlock(&lp->lock);
    return le;
}

gll_el gll_insert_el (gll_el el, void * data) {
    if (el == NULL || data == NULL)
        return NULL;
    pthread_mutex_lock(&el->ell_li->lock);

    gll_el le = (gll_el) malloc(sizeof(gll_el_ty));
    bzero((void *) le, sizeof(gll_el_ty));
    le->ell_li = el->ell_li;

    le->payload = data;

    le->next = el->next;
    el->next = le;
    le->previous = el;
    pthread_mutex_unlock(&el->ell_li->lock);
    return le;
}

gll_el gll_remove_el (gll_el el) {
    if (el == NULL)
        return NULL;
    gll_el le;
    gll_li lp = el->ell_li;

    pthread_mutex_lock(&lp->lock);

    le = lp->first;

    if (le == NULL) {
        printf("WARNING: searching for %08x when list %s was empty!\n", el, lp->name);
        pthread_mutex_unlock(&lp->lock);
        return NULL;
    }
    // search list for item to remove
    while(le != NULL) {
        if (le == el) {
            // special cases first

            if ( (le == lp->first) && (le == lp->last)) {
                // Only item in list?
                printf("only item on list\n");
                lp->first = NULL;
                lp->last = NULL;
            } else if (le == lp->first) {
                // start of list
                printf("first item on list\n");
                lp->first = le->next;
                lp->first->previous = NULL;
            } else if (le == lp->last) {
                //end of list
                printf("last item on list\n");
                lp->last = le->previous;
                lp->last->next = NULL;
            } else {
                // somewhere in the middle
                printf("somewhere in the middle\n");
                le->previous->next = le->next;
                le->next->previous = le->previous;
            }
            gll_remove_el(gll_find_el(gll_li_li,le));
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

gll_el gll_find_el (gll_li li,void *data) {
    if (li == NULL || data == NULL)
        return NULL;

    gll_el le;

    pthread_mutex_lock(&li->lock);

    le = li->first;

    if (le == NULL) {
        printf("WARNING: searching for %08x when list %s was empty!\n", le, li->name);
        pthread_mutex_unlock(&li->lock);
        return NULL;
    }
    // search list for item to remove
    while(le != NULL) {
        if (le->payload == data) {
            break;
        }
        // not found yet so keep looking
        le = le->next;
    }
    pthread_mutex_unlock(&li->lock);
    return le;
}

gll_el gll_get_first(gll_li lp) {
    if (lp == NULL)
        return NULL;
    return lp->first;
}

gll_el gll_get_last(gll_li lp) {
    if (lp == NULL)
        return NULL;
    return lp->last;
}

gll_el gll_get_next(gll_el el) {
    if (el == NULL)
        return NULL;
    return el->next;
}

gll_el gll_get_prev(gll_el el) {
    if (el == NULL)
        return NULL;
    return el->previous;
}

void *gll_get_data(gll_el el) {
    if (el == NULL)
        return NULL;
    return el->payload;
}

int gll_get_count(gll_li lp) {
    if (lp == NULL)
        return -1;

    int counter = 0;
    gll_el le = lp->first;
    while (le != NULL) {
        counter++;
        le = le->next;
    }
    return counter;
}

void gll_print_li (gll_li lp) {
    if (lp == NULL) {
        printf("NULL pointer passed to ell_print_li\n");
        return;
    }

    pthread_mutex_lock(&lp->lock);
    gll_el le;

    printf ("Dump of list called %s\n", lp->name);
    printf ("First element %08x\n", lp->first);
    printf ("Last element %08x\n", lp->last);

    if (lp->first == NULL)
        printf ("\tlist is empty!\n");
    else {
        le = lp->first;
        while (le != NULL) {
            printf ("\t%08x,", le -> payload);
            le = le -> next;
        }
    }
    printf ("\n");
    pthread_mutex_unlock(&lp->lock);
}

int gll_list () {
    gll_li lp = gll_li_li;
    gll_el le;

    printf ("%.26s\n",DASH);
    printf ("%-20s %5s\n", "List name","count");
    printf ("%-.20s %.5s\n",DASH,DASH);

    if (gll_get_count (lp) <=0)
        printf ("\tlist of lists is empty!\n");
    else {
        le = gll_get_first(lp);

        while (le != NULL) {
            lp = (gll_li) gll_get_data(le);
            printf ("%-20s %5d\n", lp->name,gll_get_count(lp));
            le = gll_get_next(le);
        }
    }
    printf ("%.26s\n",DASH);
    pthread_mutex_unlock(&lp->lock);
    return 0;
}

void gll_clear_li (gll_li lp) {
    gll_el le,tmp;
    pthread_mutex_lock(&lp->lock);

    le = lp->first;
    while (le != NULL) {
        tmp = le;
        le = le -> next;
        free(tmp);
    }
    pthread_mutex_unlock(&lp->lock);
}
