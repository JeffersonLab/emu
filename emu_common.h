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
 *      emu  - emu_common.h
 *
 *----------------------------------------------------------------------------*/
#ifndef EMU_COMMON_H_
#define EMU_COMMON_H_

#ifdef EMU_DEBUG_ON
#define EMU_DEBUG(args) { \
    printf ("LOG - %s:%d: ",__FILE__, __LINE__); \
    printf args; \
    printf("\n"); \
    };
#else
#define EMU_DEBUG(args)
#endif




#define err_abort(code,text) do { \
    fprintf (stderr, "%s at \"%s\":%d: %s\n", \
        text, __FILE__, __LINE__, strerror (code)); \
    exit (-1); \
    } while (0)

#define err_return(code,text) { \
    fprintf (stderr, "%s at \"%s\":%d: %s\n", \
        text, __FILE__, __LINE__, strerror (code)); \
    return code; \
    }

#define err_cleanup(code,text) { \
    fprintf (stderr, "%s at \"%s\":%d: %s\n", \
        text, __FILE__, __LINE__, strerror (code)); \
    goto cleanup; \
    }

#endif /*EMU_COMMON_H_*/
