/*----------------------------------------------------------------------------
 *  Copyright (c) 2007        Southeastern Universities Research Association,
 *                            Thomas Jefferson National Accelerator Facility
 *
 *    This software was developed under a United States Government license
 *    described in the NOTICE file included as part of this distribution.
 *
 *    Author:  heyes
 *    Created: Jun 15, 2007
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
 *      emu  - gparse.h
 *
 *----------------------------------------------------------------------------*/

 #ifndef GPARSE_H_
#define GPARSE_H_
typedef struct attr {
    char *tag;
    char *value;
}
attr_struct;

typedef struct element {
    char *tag;
    char *value;
    char *context;
    int n_attr;
    struct attr *attr;
}
element_struct;
extern int gparse_file(char *filename);
extern void gparse_register(char *tag_name, char *context, void (*handler_rtn)(struct element *));
#endif /*GPARSE_H_*/
