//
// Copyright 2021, Jefferson Science Associates, LLC.
// Subject to the terms in the LICENSE file found in the top-level directory.
//
// EPSCI Group
// Thomas Jefferson National Accelerator Facility
// 12000, Jefferson Ave, Newport News, VA 23606
// (757)-269-7100

package org.jlab.coda.emu.support.data;

public class TimeSliceBankItem {
    // This only holds an existing payload buffer object
    PayloadBuffer buf;

    public PayloadBuffer getBuf() {return buf;}
    public void setBuf(PayloadBuffer buffer) {buf = buffer;}
}
