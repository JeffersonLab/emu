/*----------------------------------------------------------------------------
 *  Copyright (c) 2007        Southeastern Universities Research Association,
 *                            Thomas Jefferson National Accelerator Facility
 *
 *    This software was developed under a United States Government license
 *    described in the NOTICE file included as part of this distribution.
 *
 *    Author:  heyes
 *    Created: 8 Mar 2007
 *
 *             heyes@jlab.org                    Jefferson Lab, MS-12H
 *             Phone: (757) 269-7030             12000 Jefferson Ave.
 *             Fax:   (757) 269-5800             Newport News, VA 23606
 *
 *----------------------------------------------------------------------------
 *
 * Description:
 *      emu  - emu_int_data_struct.h
 *
 *----------------------------------------------------------------------------*/
 #ifndef EMU_INT_DATA_STRUCT_H_
#define EMU_INT_DATA_STRUCT_H_

typedef struct roc_record_struct *roc_record;

typedef struct roc_record_struct {
	uint32_t recordNB;
	uint32_t rocID;
	uint32_t eventsInRecord;
	uint32_t payload[];
} roc_record_t;



#endif /*EMU_INT_DATA_STRUCT_H_*/
