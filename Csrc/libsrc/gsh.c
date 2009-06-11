/*----------------------------------------------------------------------------
 *  Copyright (c) 2007        Southeastern Universities Research Association,
 *                            Thomas Jefferson National Accelerator Facility
 *
 *    This software was developed under a United States Government license
 *    described in the NOTICE file included as part of this distribution.
 *
 *    Author:  heyes
 *    Created: Apr 30, 2007
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
 *      emu  - emu_signal_handler.c
 *
 *----------------------------------------------------------------------------*/
#include <stdio.h>
#include "gsl.h"
#include "gll.h"
#include "gkb.h"
#include "signal.h"
#include <stdlib.h>
#include <string.h>

// private stuff

static gll_li cc_help_li = NULL;

typedef struct cc_help_str *cc_help;

typedef struct  cc_help_str
{
    char *name;
    void (*handler)(void *arg);
    void *arg;
}
cc_help_ty;

void gsh_block()
{
    sigset_t      sigblockset;
    int status;
    /*************************/
    /* setup signal handling */
    /*************************/
    //sigfillset(&sigblockset);
    sigemptyset(&sigblockset);
    sigaddset(&sigblockset,SIGINT);
    status = pthread_sigmask(SIG_BLOCK, &sigblockset, NULL);
    if (status != 0)
    {
        printf("pthread_sigmask failure\n");
        exit(1);
    }

}
void gsh_create()
{
    cc_help_li = gll_create_li("Control-C handlers");
}

static void cc_handler(void *arg)
{
    /*struct gtp_thread *thread_descriptor = (struct gtp_thread *) arg;*/
    sigset_t       sigwaitset;
    int sig_num;
    gll_el le;
    sigemptyset(&sigwaitset);
    sigaddset(&sigwaitset, SIGINT);
    /* turn this thread into a signal handler */
    sigwait(&sigwaitset, &sig_num);
    // when we wake call our handlers in sequence
    le = cc_help_li->first;
    while (le != NULL)
    {
        cc_help helper;
        void (*handler)(void *arg);

        helper = (cc_help) le->payload;
        handler = helper->handler;
        printf ("\t calling handler %s\n", helper->name);
        handler(helper->arg);

        le = le->next;
    }
    gkb_stop();
    //gtp_cancel(thread_descriptor);
}

void gsh_start()
{
    gsh_block();
    //gtp_create(1,"Control-C handler", cc_handler, NULL);
}

void gsh_add(char *name,void (*handler_routine)(void *), void *arg)
{
    cc_help helper;
    if (name == NULL)
        name = "unknown control-C handler";
    helper = (cc_help) malloc(sizeof(cc_help_ty));
    helper->name = strdup(name);
    helper->handler = handler_routine;
    helper->arg = arg;

    gll_add_el(cc_help_li, helper);

}

#ifdef TEST_MAIN
#define FALSE 0
#define TRUE 1

static int done = FALSE;

void doit(void *arg)
{
    printf("\t doit called with %d \n", arg);
}
void doneit(void *arg)
{
    printf("\t doneit called with %d \n", arg);
    done = TRUE;
}

main()
{
    gsh_create();
    gsh_add("handler A",doit,(void *)1);
    gsh_add("handler B",doit,(void *)2);
    gsh_add("handler C",doit,(void *)3);
    gsh_add("handler D",doit,(void *)4);
    gsh_add("handler DONEIT",doneit,(void *)4);

    gll_print_li(cc_help_li);

    gsh_start();

    while(done == FALSE)
    {
        gsl_sleep(2);
    }
}

#endif

