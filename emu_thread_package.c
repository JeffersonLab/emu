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
 *      emu  - emu_thread_package.c
 *
 *----------------------------------------------------------------------------*/

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <sys/types.h>
#include <sys/mman.h>
#include <signal.h>
#include <unistd.h>
#include <time.h>
#include <dlfcn.h>
#ifdef sun
#include <thread.h>
#endif
#include <pthread.h>

#include "emu_common.h"
#include "emu_thread_package.h"

/* Lock or list of active threads lock */
pthread_mutex_t thread_list_lock = PTHREAD_MUTEX_INITIALIZER;

static struct emu_thread *emu_thread_thread_list = (struct emu_thread *) NULL;
static int thread_count = 0;

/*
 * author heyes
 *
 * To change this generated comment edit the template variable &quot;comment&quot;:
 */

int emu_create_thread(int detatched,char *name, void *thread_body, void *thread_args)
{
    int status;
    struct emu_thread *thread_descriptor;

    thread_descriptor = (struct emu_thread *) malloc(sizeof(struct emu_thread));

    bzero(thread_descriptor, sizeof(struct emu_thread));

    thread_descriptor->name = malloc(strlen(name)+1);

    strcpy(thread_descriptor->name, name);

    /* get thread attribute ready */
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

    pthread_mutex_unlock(&thread_list_lock);

    return 0;
cleanup:
    pthread_attr_destroy(&thread_descriptor->attr);
    free(thread_descriptor->name);
    free(thread_descriptor);
    return -1;
}

void emu_thread_cleanup(struct emu_thread *thread_descriptor)
{
    thread_descriptor->status = EMU_THREAD_ENDED;
    pthread_exit(NULL);
}

static int monitor_threads = 1;

void emu_thread_monitor()
{
    struct timespec waitforme, monitor, beat;

    /* set some useful timeout periods */
    waitforme.tv_sec  = 5;
    waitforme.tv_nsec = 0; /* 500 millisec */

    while (monitor_threads)
    {
        if  (emu_thread_thread_list != NULL)
        {
            struct emu_thread *thread_descriptor;

            thread_descriptor = emu_thread_thread_list;

            while (thread_descriptor != NULL)
            {
                struct emu_thread *emu_thread_next = thread_descriptor->next;
                //printf("Thread named %s has state %d\n", thread_descriptor->name, thread_descriptor->status);
                if (thread_descriptor->status == EMU_THREAD_ENDED)
                {
                    printf( "  - thread %s has ended\n", thread_descriptor->name);
                    pthread_mutex_lock(&thread_list_lock);
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
                    printf("   - thread resources freed %d threads left\n",thread_count);

                    pthread_mutex_unlock(&thread_list_lock);
                }
                thread_descriptor = emu_thread_next;
            }
        }
        nanosleep(&waitforme, NULL);
    }
    printf("All threads terminated normally, exiting monitor\n");

}

void emu_wait_thread_end()
{
    // it's really this simple...
    pthread_exit(NULL);
}

void emu_start_thread_monitor()
{
    pthread_attr_t  attr;

    pthread_t thread_id;
    monitor_threads = 1;

    pthread_attr_init(&attr);
    pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED);
    pthread_create(&thread_id, &attr, (void *) emu_thread_monitor, NULL);
}

void emu_stop_thread_monitor()
{
    monitor_threads = 0;
}

//end of file
