/*----------------------------------------------------------------------------
 *  Copyright (c) 2007        Southeastern Universities Research Association,
 *                            Thomas Jefferson National Accelerator Facility
 *
 *    This software was developed under a United States Government license
 *    described in the NOTICE file included as part of this distribution.
 *
 *    Author:  heyes
 *    Created: Apr 17, 2007
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
 *      emu  - generic_keyboard_control.c
 *
 * This file contains a pacakge to implement keyboard control of a program.
 * Useful for debugging etc.
 *
 *----------------------------------------------------------------------------*/

#include <stdlib.h>
#include <stdio.h>
#include <strings.h>
#include <sys/types.h>
#include <unistd.h>
#include "emu_thread_package.h"
#include "ell.h"

typedef struct key_function *key_function_ptr;

typedef struct key_function
{
    char key;
    int (*handler)();
    void *arg;
    char *help;

}
key_function_struct;

ell_li key_function_list = NULL;

void GKB_add_key(char key, int (*handler)(),void *arg, char *help)
{
    key_function_ptr p = (key_function_ptr) malloc(sizeof(key_function_struct));

    bzero ((void *) p, sizeof(key_function_struct));

    if (key_function_list == NULL)
        key_function_list = ell_create_li("Keybd cmd handlers");

    p->help = strdup(help);
    p->key = key;
    p->handler = handler;
    p->arg = arg;

    ell_add_el(key_function_list,p);
}

int GKB_print_help()
{
    ell_el el = ell_get_first(key_function_list);
    key_function_ptr p;

    printf("Help ----\n");

    while( el !=NULL)
    {
        p = ell_get_data(el);
        printf("   %c : %s\n",p->key,p->help);
        el = ell_get_next(el);
    }
    printf("---------\n\n");
    return 0;
}

int GKB_simple_quit()
{
    return -1;
}

void GKB_handler(void *arg)
{
    // arg is not used but needs to be there so we can be a thread

    while (1)
    {
        char key = getchar();
        key_function_ptr p;
        ell_el el = ell_get_first(key_function_list);
        int status;
        while (el != NULL)
        {
            p = ell_get_data(el);
            if (p->key == key)
            {
                status = (int) (*(p->handler))(p->arg);
                if (status < 0)
                {
                    emu_thread_cleanup(NULL);
                    return;
                }
            }
            el = ell_get_next(el);

        }
    }

}

static struct emu_thread *GKB_monitor = NULL;

void GKB_start()
{
    if (GKB_monitor == NULL)
        GKB_monitor = emu_create_thread(0,"Key catcher", GKB_handler, NULL);
}

void GKB_stop()
{
    if (GKB_monitor != NULL)
        emu_thread_cleanup(GKB_monitor);
}
//// test code
//
//int do_something()
//{
//    printf ("hello world\n");
//    return 0;
//}
//
//int main(int argc,char **argv)
//{
//    GKB_add_key('h', GKB_print_help, "Print out this help message");
//    GKB_add_key('x', do_something, "Do something");
//
//    GKB_add_key('q', GKB_simple_quit, "quit");
//    GKB_handler(NULL);
//    return 0;
//}
