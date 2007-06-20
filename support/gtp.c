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
 *    URL : $HeadURL: file:///daqfs/source/svnroot/coda/3.0/emu/emu_thread_package.c $
 *
 *             heyes@jlab.org                    Jefferson Lab, MS-12H
 *             Phone: (757) 269-7030             12000 Jefferson Ave.
 *             Fax:   (757) 269-5800             Newport News, VA 23606
 *
 *----------------------------------------------------------------------------
 *
 * Description:
 * emu  - emu_thread_package.c
 * This package encapsulates the creation, monitoring and deletion of threads.
 * A thread is created by gtp_create and it is associated with a thread_descriptor.
 * The thread descriptors are placed in a linked list.
 * When a thread exits it calls gtp_cancel and passes it's own descriptor as the argument.
 * To stop another thread gtp_cancel is called with that thread's descriptor.
 * A routine emu_thread_monitor can be called from the main program (it blocks until all threads
 * are ended) or started as a thread using emu_start_thread_monitor. This routine checks the list
 * of threads looking for any that have been the target of gtp_cancel. It removes the thread
 * descriptor from the list and frees up associated storage.
 *----------------------------------------------------------------------------*/

#include "gtp.h"
#include "gll.h"

static gll_li thread_list = (gll_li) NULL;
static int monitor_threads = 1;

#define DASH  "------------------------------------------"
#define err_cleanup(code,text) { \
    fprintf (stderr, "%s at \"%s\":%d: %s\n", \
        text, __FILE__, __LINE__, strerror (code)); \
    goto cleanup; \
    }

int gtp_list() {
    struct gtp_thread *thread_descriptor;
    gll_el el;
    el = gll_get_first(thread_list);

    printf("%.35s\n",DASH);
    printf("%-8s %-26s\n","Thread","Name");
    printf("%.8s %.26s\n",DASH,DASH);


    while (el != NULL) {
        /*List isn't empty*/
        thread_descriptor = (struct gtp_thread *) gll_get_data(el);

        printf ("%08x %-26.26s\n",thread_descriptor->thread_id,thread_descriptor->name);
        el = gll_get_next(el);
    }
    printf("%.35s\n",DASH);
    printf("%d threads in total\n",gll_get_count(thread_list));
    printf("%.35s\n",DASH);
}

/* gtp_create.
 * Create and return a wrapper for a thread.
 * Start the thread and pass the thread arguments. The thread body should call
 * gtp_cancel. The argument passed to the thread body is the thread descriptor.
 */


struct gtp_thread *gtp_create(int detatched,char *name, void *thread_body, void *thread_args) {
    int status;
    struct gtp_thread *thread_descriptor;

    if (thread_list == NULL) {

#ifdef sun

        int		  con, con_add;

        /*
         * want one thread for each station's conductor + extra threads
         * for heartbeat production and detection + 1 for main thd +
         * 1 for add stations thd + 2 for udp & tcp threads. However,
         * if we exceed system resources, we'll need to try something
         * more conservative.
         */
        con = thr_getconcurrency();
        if (thr_setconcurrency(con + config->nstations + ET_EXTRA_THREADS + 4) != 0) {
            /* exceeded # of threads allowed so reduce and try again */
            if (thr_setconcurrency(config->nstations + ET_EXTRA_THREADS + 4) != 0) {
                /* exceeded # of threads allowed so try fixed number */
                if (thr_setconcurrency(20) != 0) {
                    /* exceeded # of threads allowed so let system choose */
                    thr_setconcurrency(0);
                }
            }
        }
        con_add = thr_getconcurrency() - con;
        if (con_add < 1) {
            con_add = 0;
        }
#endif
        thread_list = gll_create_li("Thread list");
    }

    thread_descriptor = (struct gtp_thread *) malloc(sizeof(struct gtp_thread));

    bzero(thread_descriptor, sizeof(struct gtp_thread));

    thread_descriptor->name = malloc(strlen(name)+1);

    strcpy(thread_descriptor->name, name);

    // get thread attribute ready
    status = pthread_attr_init(&thread_descriptor->attr);

    if(status != 0) {
        err_cleanup(status, "Init thd attr %s");
    }

    if (detatched) {
        status = pthread_attr_setdetachstate(&thread_descriptor->attr, PTHREAD_CREATE_DETACHED);
        if(status != 0) {
            err_cleanup(status, "Set thd detach %s");
        }
    }

    thread_descriptor->args = thread_args;
    thread_descriptor->status = _THREAD_STARTING;

    status = pthread_create(&thread_descriptor->thread_id, &thread_descriptor->attr, thread_body, (void *) thread_descriptor);

    if (status != 0) {
        err_cleanup(status, "Create Failed");
    }

    gll_add_el(thread_list, thread_descriptor);

    return thread_descriptor;


cleanup:
    pthread_attr_destroy(&thread_descriptor->attr);
    free(thread_descriptor->name);
    free(thread_descriptor);
    return NULL;
}

/* gtp_enable_cancel
 */

void gtp_enable_cancel() {
    int old;

    pthread_setcancelstate(PTHREAD_CANCEL_ENABLE,&old);

    pthread_setcanceltype(PTHREAD_CANCEL_ASYNCHRONOUS,&old);
}

/* gtp_cancel.
 * cleanly end the thread described in the thread_descriptor.
 */

void gtp_cancel(struct gtp_thread *thread_descriptor) {
    pthread_t thread_id;
    void *status;
    struct gtp_thread *emu_thread_next;

    gll_el el;

    if (thread_descriptor == NULL) {
        el = gll_get_first(thread_list);
        while (el != NULL) {
            /*List isn't empty*/
            thread_descriptor = (struct gtp_thread *) gll_get_data(el);

            if (thread_descriptor->thread_id != pthread_self()) {
                gll_remove_el(el);
                gtp_list();
                break;
            }
            el = gll_get_next(el);
        }

        pthread_exit(NULL);
        if (el == NULL)
            return;
    } else {
        thread_id = thread_descriptor->thread_id;

        // lock out other threads while we tweak the list
        el = gll_get_first(thread_list);
        while (el != NULL) {
            thread_descriptor = (struct gtp_thread *) gll_get_data(el);
            if (thread_id == thread_descriptor->thread_id) {
                break;
            }
            el = gll_get_next(el);
        }
        if (el == NULL) {
            return;
        } else
            gll_remove_el(el);

        /* If we are cancelling another thread we can't free the descriptor until
         * we are sure that it isn't in use so cancel the thread then call join to wait
         * for the thread to die.
        */

        pthread_cancel(thread_id);
        //pthread_join(thread_id, &status);

    }
    pthread_attr_destroy(&thread_descriptor->attr);

    free(thread_descriptor->name);

    free(thread_descriptor);
    gtp_list();


}


/* emu_wait_thread_end
 * A main thread of the program may want to start the monitor as a thread then just wait for it to end.
 * it does this by calling pthread_exit which waits for all threads to exit then exits the
 * main thread
 */

void emu_wait_thread_end() {
    // it's really this simple...
    pthread_exit(NULL);
}



//end of file
