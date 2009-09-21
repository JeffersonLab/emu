/*
 * Copyright (c) 2008, Jefferson Science Associates
 *
 * Thomas Jefferson National Accelerator Facility
 * Data Acquisition Group
 *
 * 12000, Jefferson Ave, Newport News, VA 23606
 * Phone : (757)-269-7100
 *
 */
package org.jlab.coda.support.ui.log;

import javax.swing.text.DefaultCaret;
import java.awt.*;

/** @author Michael Lifshits */
public class LogTextCaret extends DefaultCaret {

    /** Field serialVersionUID */
    private static final long serialVersionUID = 1L;

    /** Field visibilityAdjustmentEnabled */
    private boolean visibilityAdjustmentEnabled = true;

    /**
     * Method adjustVisibility ...
     *
     * @param nloc of type Rectangle
     */
    protected void adjustVisibility(Rectangle nloc) {
        if (visibilityAdjustmentEnabled) {
            super.adjustVisibility(nloc);
        }
    }

    /**
     * Method setVisibilityAdjustment sets the visibilityAdjustment of this LogTextCaret object.
     *
     * @param flag the visibilityAdjustment of this LogTextCaret object.
     */
    public void setVisibilityAdjustment(boolean flag) {
        visibilityAdjustmentEnabled = flag;
    }

}
