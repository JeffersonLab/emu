/*----------------------------------------------------------------------------
 *  Copyright (c) 2007        Southeastern Universities Research Association,
 *                            Thomas Jefferson National Accelerator Facility
 *
 *    This software was developed under a United States Government license
 *    described in the NOTICE file included as part of this distribution.
 *
 *    Author:  heyes
 *    Created: Jun 6, 2007
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
 *      emu  - gph.c
 *
 * 		A generic library to maintain a named list of parameters and associated info.
 *----------------------------------------------------------------------------*/

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include "gph.h"

#define DASH  "-----------------------------------------"

static gll_li param_list = NULL;

void gph_internal_add(int type,char *name,char key,char * help,void *value) {
    if (param_list == NULL) {
        param_list = gll_create_li("Global Parameters");
    }

    // what to do if the parameter already exists?
    if (type == GPH_VALUE) {
        if (gph_set_value(name, value) == 0 )
            return;
    } else {
        if (gph_set_param(name, value) == 0 )
            return;
    }
    // it didn't exist so create it
    gph_param p = (gph_param) malloc(sizeof(gph_param_ty));

    bzero ((void *) p, sizeof(gph_param_ty));

    p->name = strdup(name);
    if (type == GPH_VALUE) {
        p->value = value;
    } else {
        p->value = strdup(value);
    }
    if (key != GPH_NO_KEY) {
        p->help = strdup(help);
    }
    p->key = key;
    p->type = type;
    gll_add_el(param_list,(void *) p);

}


void gph_add_value(char *name, void *pointer) {
    gph_internal_add(GPH_VALUE,name,GPH_NO_KEY,NULL,pointer);
}

void gph_add_param(char *name,char key,char *help,void *initial) {
    gph_internal_add(GPH_PARAM,name,key,help,initial);
}
void gph_add_const(char *name,char key,char *help,void *value) {
    gph_internal_add(GPH_CONST,name,key,help,value);
}

void gph_del_param(char *name) {
    if (param_list == NULL)
        return;

    gll_el el = gll_get_first(param_list);
    gll_el el_next;

    while (el != NULL) {

        gph_param p = gll_get_data(el);
        el_next = gll_get_next(el);
        if (strcmp(p->name,name) == 0) {
            gll_remove_el(el);
            free(p->name);

            if (p->type != GPH_VALUE)
                free(p->value);
            free(p);
            break;
        }
        el = el_next;
    }

}

int gph_set_param(char *name, char *value) {
    if (param_list == NULL)
        return;

    gll_el el = gll_get_first(param_list);

    while (el != NULL) {
        gph_param p = gll_get_data(el);
        if ((strcmp(p->name,name) == 0) && (p->type != GPH_VALUE)) {
            free(p->value);
            p->value= strdup(value);
            return 0;
        }
        el = gll_get_next(el);
    }
    return -1;
}

int gph_set_value(char *name, char *value) {
    if (param_list == NULL)
        return;

    gll_el el = gll_get_first(param_list);

    while (el != NULL) {
        gph_param p = gll_get_data(el);
        if ((strcmp(p->name,name) == 0) && (p->type == GPH_VALUE)) {
            p->value = value;
            return 0;
        }
        el = gll_get_next(el);
    }
    return -1;
}

int gph_set_keyed_param(int key, char *value) {
    if (param_list == NULL)
        return;

    gll_el el = gll_get_first(param_list);

    while (el != NULL) {
        gph_param p = gll_get_data(el);
        if (p->key == key) {
            free(p->value);
            p->value = strdup(value);
            return 0;
        }
        el = gll_get_next(el);
    }
    return -1;
}

char *gph_get_param(char *name) {
    if (param_list == NULL)
        return NULL;

    gll_el el = gll_get_first(param_list);

    while (el != NULL) {
        gph_param p = gll_get_data(el);
        if (strcmp(p->name,name) == 0) {
            return p->value;
        }
        el = gll_get_next(el);
    }
    return NULL;
}

void *gph_get_value(char *name) {
    if (param_list == NULL)
        return NULL;

    gll_el el = gll_get_first(param_list);

    while (el != NULL) {
        gph_param p = gll_get_data(el);
        if (strcmp(p->name,name) == 0) {
            return p->value;
        }
        el = gll_get_next(el);
    }
    return NULL;
}

extern char  *optarg;
extern int    optind;


char * gph_get_help() {
    char *help_string = "";
    if (param_list == NULL)
        return help_string;

    gll_el el = gll_get_first(param_list);

    while (el != NULL) {
        gph_param p = gll_get_data(el);
        if (p->key != GPH_NO_KEY) {
            if (strcmp(help_string,"") !=0) {
                char *tmp = malloc(strlen(help_string) + strlen(p->help)+10);
                sprintf(tmp,"%s      -%c : %s\n",help_string,p->key,p->help);
                free(help_string);
                help_string = tmp;
            } else {
                help_string = malloc(strlen(p->help)+10);
                sprintf(help_string,"      -%c : %s\n",p->key , p->help);
            }
        }
        el = gll_get_next(el);
    }
    return help_string;

}

void gph_list_parameters() {

    if (param_list == NULL)
        return;

    gll_el el = gll_get_first(param_list);
    printf ("%.30s\n",DASH);
    printf ("%-20s   %7s\n", "Param name","value");
    printf ("%-.20s   %.7s\n",DASH,DASH);
    while (el != NULL) {
        gph_param p = gll_get_data(el);
        if (p->type == GPH_VALUE) {
            printf("%-20s : %08x\n", p->name,p->value);
        } else {
            printf("%-20s : %-60s\n", p->name,p->value);
        }
        el = gll_get_next(el);
    }

    printf ("%.30s\n",DASH);

}


char * gph_get_keys() {
    char *key_string = "";
    if (param_list == NULL)
        return key_string;

    gll_el el = gll_get_first(param_list);

    while (el != NULL) {
        gph_param p = gll_get_data(el);
        if (p->key != GPH_NO_KEY) {
            if (strcmp(key_string,"") !=0) {
                char *tmp = malloc(strlen(key_string) + 3);
                sprintf(tmp,"%s%c:",key_string,p->key);
                free(key_string);
                key_string = tmp;
            } else {
                key_string = malloc(4);
                sprintf(key_string,"%c:",p->key);
            }
        }
        el = gll_get_next(el);
    }
    return key_string;
}

void gph_cmd_line(int argc,char **argv) {
    int           c;

    gkb_add_key('P', gph_list_parameters, NULL,"list parameters");

    while ((c = getopt(argc, argv, gph_get_keys())) != EOF) {
        gph_set_keyed_param(c,optarg);
    }

}

void gph_print_help() {
    fprintf(stderr, "\n%s\n", gph_get_help());
    fprintf(stderr, "\n%s\n", gph_get_keys());
}
