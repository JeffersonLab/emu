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
 * emu  - emu_thread_package.c
 * This package encapsulates the creation, monitoring and deletion of threads.
 * A thread is created by emu_create_thread and it is associated with a thread_descriptor.
 * The thread descriptors are placed in a linked list.
 * When a thread exits it calls emu_thread_cleanup and passes it's own descriptor as the argument.
 * To stop another thread emu_thread_cleanup is called with that thread's descriptor.
 * A routine emu_thread_monitor can be called from the main program (it blocks until all threads
 * are ended) or started as a thread using emu_start_thread_monitor. This routine checks the list
 * of threads looking for any that have been the target of emu_thread_cleanup. It removes the thread
 * descriptor from the list and frees up associated storage.
 *----------------------------------------------------------------------------*/

#include "emu_thread_package.h"
#include "ell.h"

static ell_li emu_thread_list = (ell_li) NULL;
static int monitor_threads = 1;

int emu_list_threads()
{
    struct emu_thread *thread_descriptor;
    ell_el el;
    el = ell_get_first(emu_thread_list);
    printf("Thread list -------------------------------------\n");
    while (el != NULL)
    {
        /*List isn't empty*/
        thread_descriptor = (struct emu_thread *) ell_get_data(el);

        printf ("thread %08x named : %s\n",thread_descriptor->thread_id,thread_descriptor->name);
        el = ell_get_next(el);
    }
    printf("------------------------------------------------\n");
    printf("%d threads in total\n\n",ell_get_count(emu_thread_list));
}

/* emu_create_thread.
 * Create and return a wrapper for a thread.
 * Start the thread and pass the thread arguments. The thread body should call
 * emu_thread_cleanup. The argument passed to the thread body is the thread descriptor.
 */


struct emu_thread *emu_create_thread(int detatched,char *name, void *thread_body, void *thread_args)
{
    int status;
    struct emu_thread *thread_descriptor;

    if (emu_thread_list == NULL)
        emu_thread_list = ell_create_li("Thread list");

    thread_descriptor = (struct emu_thread *) malloc(sizeof(struct emu_thread));

    bzero(thread_descriptor, sizeof(struct emu_thread));

    thread_descriptor->name = malloc(strlen(name)+1);

    strcpy(thread_descriptor->name, name);

    // get thread attribute ready
    status = pthread_attr_init(&thread_descriptor->attr);

    if(status != 0)
    {
        err_cleanup(status, "Init thd attr %s");
    }

    if (detatched)
    {
        status = pthread_attr_setdetachstate(&thread_descriptor->attr, PTHREAD_CREATE_DETACHED);
        if(status != 0)
        {
            err_cleanup(status, "Set thd detach %s");
        }
    }

    EMU_DEBUG(("Create thread %s\n", name));

    thread_descriptor->args = thread_args;
    thread_descriptor->status = EMU_THREAD_STARTING;

    status = pthread_create(&thread_descriptor->thread_id, &thread_descriptor->attr, thread_body, (void *) thread_descriptor);

    if (status != 0)
    {
        err_cleanup(status, "Create Failed");
    }

    ell_add_el(emu_thread_list, thread_descriptor);

    return thread_descriptor;


cleanup:
    pthread_attr_destroy(&thread_descriptor->attr);
    free(thread_descriptor->name);
    free(thread_descriptor);
    return NULL;
}

/* emu_enable_cancel
 */

void emu_enable_cancel()
{
    int old;

    pthread_setcancelstate(PTHREAD_CANCEL_ENABLE,&old);

    pthread_setcanceltype(PTHREAD_CANCEL_ASYNCHRONOUS,&old);
}

/* emu_thread_cleanup.
 * cleanly end the thread described in the thread_descriptor.
 */

void emu_thread_cleanup(struct emu_thread *thread_descriptor)
{
    pthread_t thread_id;
    void *status;
    struct emu_thread *emu_thread_next;

    ell_el el;

    if (thread_descriptor == NULL)
    {
        el = ell_get_first(emu_thread_list);
        while (el != NULL)
        {
            /*List isn't empty*/
            thread_descriptor = (struct emu_thread *) ell_get_data(el);

            if (thread_descriptor->thread_id != pthread_self())
            {
                EMU_DEBUG(("Thread %08x (%s) is this thread and will be cleaned up",thread_id, thread_descriptor->name));
                ell_remove_el(el);
                emu_list_threads();
                break;
            }
            el = ell_get_next(el);
        }

        pthread_exit(NULL);
        if (el == NULL)
            return;
    }
    else
    {
        thread_id = thread_descriptor->thread_id;
        EMU_DEBUG(("Attempt cleanup of thread (%08x)",thread_id));
        // lock out other threads while we tweak the list
        el = ell_get_first(emu_thread_list);
        while (el != NULL)
        {
            thread_descriptor = (struct emu_thread *) ell_get_data(el);
            if (thread_id == thread_descriptor->thread_id)
            {
                break;
            }
            el = ell_get_next(el);
        }
        if (el == NULL)
        {
            EMU_DEBUG(("Thread %08x not found. Already cleaned up",thread_id));
            return;
        }
        else
            ell_remove_el(el);

        /* If we are cancelling another thread we can't free the descriptor until
         * we are sure that it isn't in use so cancel the thread then call join to wait
         * for the thread to die.
        */

        pthread_cancel(thread_id);
        //pthread_join(thread_id, &status);

    }
    pthread_attr_destroy(&thread_descriptor->attr);
    EMU_DEBUG(("free %08x",thread_descriptor->name));
    free(thread_descriptor->name);
    EMU_DEBUG(("free %08x",thread_descriptor));
    free(thread_descriptor);
    printf("   - thread resources freed %d threads left\n nemaining threads are -\n",ell_get_count(emu_thread_list));
    emu_list_threads();


}


/* emu_wait_thread_end
 * A main thread of the program may want to start the monitor as a thread then just wait for it to end.
 * it does this by calling pthread_exit which waits for all threads to exit then exits the
 * main thread
 */

void emu_wait_thread_end()
{
    // it's really this simple...
    pthread_exit(NULL);
}



//end of file
