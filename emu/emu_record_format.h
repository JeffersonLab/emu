/*----------------------------------------------------------------------------
 *  Copyright (c) 2007        Southeastern Universities Research Association,
 *                            Thomas Jefferson National Accelerator Facility
 *
 *    This software was developed under a United States Government license
 *    described in the NOTICE file included as part of this distribution.
 *
 *    Author:  heyes
 *    Created: 8 Mar 2007
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
 *      emu  - emu_int_data_struct.h
 *
 *----------------------------------------------------------------------------*/
#ifndef EMU_INT_DATA_STRUCT_H_
#define EMU_INT_DATA_STRUCT_H_

typedef  union emu_data_record *emu_data_record_ptr;

typedef union emu_data_record {
    struct roc_record_struct
    {
        uint32_t length;
        uint32_t recordNB;
        uint32_t rocID;
        uint32_t eventsInRecord;
        uint32_t payload[];
    } record_header;

    struct roc_record_data
    {
        uint32_t length;
    	uint32_t data[];
    } record_data;
} emu_data_record_union;






#endif /*EMU_INT_DATA_STRUCT_H_*/
