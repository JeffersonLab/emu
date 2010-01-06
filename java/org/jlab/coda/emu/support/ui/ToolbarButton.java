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

package org.jlab.coda.emu.support.ui;

import javax.swing.*;
import java.awt.*;
import java.net.URL;

/**
 * <pre>
 * Class <b>ToolbarButton </b>
 * </pre>
 *
 * @author heyes
 *         Created on Sep 17, 2008
 */
class ToolbarButton extends JButton {
    /** Field margins */
    private static final Insets margins = new Insets(0, 0, 0, 0);

    /**
     * Constructor ToolbarButton creates a new ToolbarButton instance.
     *
     * @param icon of type Icon
     */
    private ToolbarButton(Icon icon) {
        super(icon);
        setMargin(margins);
        setVerticalTextPosition(BOTTOM);
        setHorizontalTextPosition(CENTER);
    }

    /**
     * Constructor ToolbarButton creates a new ToolbarButton instance.
     *
     * @param imageFile of type String
     */
    public ToolbarButton(String imageFile) {
        super();
        ClassLoader cldr = this.getClass().getClassLoader();
        URL imageURL = cldr.getResource(imageFile);
        ImageIcon icon = new ImageIcon(imageURL);
        this.setIcon(icon);
        //setText(text);
    }

    /**
     * Constructor ToolbarButton creates a new ToolbarButton instance.
     *
     * @param action of type Action
     */
    public ToolbarButton(Action action) {
        super(action);
        setMargin(margins);
        setVerticalTextPosition(BOTTOM);
        setHorizontalTextPosition(CENTER);
    }
}
