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
/* List of active threads lock */

pthread_mutex_t thread_list_lock = PTHREAD_MUTEX_INITIALIZER;

static struct emu_thread *emu_thread_thread_list = (struct emu_thread *) NULL;
static int thread_count = 0;
static int monitor_threads = 1;

int emu_thread_list()
{
    struct emu_thread *thread_descriptor;
    thread_descriptor = emu_thread_thread_list;
	printf("list head is %08x\n", thread_descriptor);
    while (thread_descriptor != NULL)
    {
        printf ("thread %08x named : %s\n",thread_descriptor->thread_id,thread_descriptor->name);
        thread_descriptor = thread_descriptor->next;
    }
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


    // linked list of thread descriptors is protected by a mutex
    pthread_mutex_lock(&thread_list_lock);
    if (emu_thread_thread_list == NULL)
    {
        // list is empty so we become first on list
        emu_thread_thread_list = thread_descriptor;

        thread_descriptor->next = NULL;
        thread_descriptor->prev = NULL;
    }
    else
    {
        // add to head of list
        emu_thread_thread_list->prev = thread_descriptor;
        thread_descriptor->next = emu_thread_thread_list;
        thread_descriptor->prev = NULL;

        emu_thread_thread_list = thread_descriptor;

    }

    thread_count++;
    // done tweaking the list so free the mutex.
    pthread_mutex_unlock(&thread_list_lock);

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


    if (thread_descriptor == NULL)
    {
        // lock out other threads while we tweak the list
        pthread_mutex_lock(&thread_list_lock);
        thread_descriptor = emu_thread_thread_list;
        while (thread_descriptor != NULL && thread_descriptor->thread_id != pthread_self())
        {
            thread_descriptor = thread_descriptor->next;
        }
        if (thread_descriptor != NULL)
        {
            EMU_DEBUG(("Attempt cleanup of thread %8x (%s)",pthread_self(), thread_descriptor->name));
            thread_descriptor->status = EMU_THREAD_ENDED;
        }
        EMU_DEBUG(("Thread %08x (%s) is this thread and cleaned up",thread_id, thread_descriptor->name));
	    emu_thread_list();
        // done tweaking the list
        pthread_mutex_unlock(&thread_list_lock);

        pthread_exit(NULL);
    }
    else
    {
        pthread_mutex_lock(&thread_list_lock);
        thread_id = thread_descriptor->thread_id;
        EMU_DEBUG(("Attempt cleanup of thread %08x",thread_id));
        // lock out other threads while we tweak the list
        thread_descriptor = emu_thread_thread_list;
        while (thread_descriptor != NULL && thread_descriptor->thread_id != thread_id)
        {
            thread_descriptor = thread_descriptor->next;
        }
        if (thread_descriptor == NULL)
        {
            EMU_DEBUG(("Thread %08x not found. Already cleaned up",thread_id));
            pthread_mutex_unlock(&thread_list_lock);
            return;
        }

        /* If we are cancelling another thread we can't free the descriptor until
         * we are sure that it isn't in use so cancel the thread then call join to wait
         * for the thread to die.
        */

        pthread_cancel(thread_id);
        //pthread_join(thread_id, &status);


        thread_descriptor->status = EMU_THREAD_ENDED;
        EMU_DEBUG(("Thread %08x (%s) found and cleaned up",thread_id, thread_descriptor->name));

        // done tweaking the list
        pthread_mutex_unlock(&thread_list_lock);
    }
}

/* emu_thread_monitor
 * This thread loops while there are threads to monitor and looks for thread
 * descriptors that belong to threads that have ended. It then removes the descriptor
 * from the list and frees any associated storage.
 */

void emu_thread_monitor()
{

    while (monitor_threads)
    {
        if  (emu_thread_thread_list != NULL)
        {
            struct emu_thread *thread_descriptor;

            // lock down the list while we play
            pthread_mutex_lock(&thread_list_lock);
            thread_descriptor = emu_thread_thread_list;

            // With the list locked work down the list checking the status
            //of each thread.
            while (thread_descriptor != NULL)
            {
                struct emu_thread *emu_thread_next = thread_descriptor->next;
                //printf("Thread named %s has state %d\n", thread_descriptor->name, thread_descriptor->status);
                if (thread_descriptor->status == EMU_THREAD_ENDED)
                {
                    printf( "  - thread %s has ended\n", thread_descriptor->name);

                    // are we at the head of the list
                    if (thread_descriptor == emu_thread_thread_list)
                    {
                        /* yes
                        * Then move the next thread along to the head of the list
                        * If there was no next thread then exit the monitor.
                        * If there was another thread then, since it's at the top of
                        * the list set prev = NULL;
                        */

                        printf("     thread was head of list\n");
                        emu_thread_thread_list = emu_thread_next;
                        if (emu_thread_thread_list == NULL)
                            monitor_threads=0;
                        else
                            emu_thread_thread_list->prev = NULL;

                    }
                    else
                    {
                        /* No
                        * We are in the middle of the list or the tail.
                        * Since we are not at the head prev != NULL so we set the next field of the
                        * thread above us to the address of the thread below us. If there is a thread
                        * below us then we set it's prev field to point to the thread above us.
                        * Now the thread above us points to the one below us and the one below us
                        * points to the one above us. Nobody points to us any more so we're off the list!
                        */

                        thread_descriptor->prev->next = thread_descriptor->next;
                        if (thread_descriptor->next != NULL)
                            thread_descriptor->next->prev = thread_descriptor->prev;

                    }

                    pthread_attr_destroy(&thread_descriptor->attr);
                    EMU_DEBUG(("free %08x",thread_descriptor->name));
                    free(thread_descriptor->name);
                    EMU_DEBUG(("free %08x",thread_descriptor));
                    free(thread_descriptor);
                    thread_count--;
                    printf("   - thread resources freed %d threads left\n nemaining threads are -\n",thread_count);
                    emu_thread_list();

                }
                thread_descriptor = emu_thread_next;
            }
            // unlock list
            pthread_mutex_unlock(&thread_list_lock);
        }
        // sleep for a while and repeat until there are no more threads to monitor.
        emu_sleep(2);
    }
    printf("All threads terminated normally, exiting monitor\n");

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

/* emu_start_thread_monitor
 * Start emu_thread_monitor as a thread
 */

void emu_start_thread_monitor()
{
    pthread_attr_t  attr;

    pthread_t thread_id;
    monitor_threads = 1;

    pthread_attr_init(&attr);
    pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED);
    pthread_create(&thread_id, &attr, (void *) emu_thread_monitor, NULL);
}

/* emu_stop_thread_monitor
 * Stops emu_thread_monitor
 */

void emu_stop_thread_monitor()
{
    monitor_threads = 0;
}


//end of file
