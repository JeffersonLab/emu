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
 *      emu  - gph.h
 *
 *----------------------------------------------------------------------------*/

#ifndef GPH_H_
#define GPH_H_
#include "gll.h"

#define GPH_NO_KEY 0
#define GPH_PARAM 0
#define GPH_CONST 1
#define GPH_VALUE 1

typedef struct gph_param_str *gph_param;

typedef struct gph_param_str {
    char *name;
    int type;
    char  key;
    char *help;
    void *value;
}
gph_param_ty;
#ifdef __cplusplus
extern "C" void gph_add_value(char *context, char *name, void *value);
extern "C" void gph_add_param(char *context, char *name,char key,char *help,void *value);
extern "C" void gph_add_const(char *context, char *name,char key,char *help,void *value);
extern "C" void gph_del_param(char *context, char *name);
extern "C" int gph_set_param(char *context, char *name, char *value);
extern "C" int gph_set_keyed_param(int key, char *value);
extern "C" char *gph_get_param(char *context, char *name);
extern "C" void *gph_get_value(char *context, char *name);
extern "C" int gph_set_value(char *context,char *name, char *value);
extern "C" void gph_cmd_line(int argc,char **argv);
extern "C" char *gph_get_help();
extern "C" void gph_print_help();
#else
extern void gph_add_value(char *context, char *name, void *value);
extern void gph_add_param(char *context, char *name,char key,char *help,void *value);
extern void gph_add_const(char *context, char *name,char key,char *help,void *value);
extern void gph_del_param(char *context, char *name);
extern int gph_set_param(char *context, char *name, char *value);
extern int gph_set_keyed_param(int key, char *value);
extern char *gph_get_param(char *context, char *name);
extern void *gph_get_value(char *context, char *name);
extern int gph_set_value(char *context,char *name, char *value);
extern void gph_cmd_line(int argc,char **argv);
extern char *gph_get_help();
extern void gph_print_help();
#endif
#endif /*GPH_H_*/
