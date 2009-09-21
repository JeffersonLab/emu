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

package org.jlab.coda.support.configurer;

import org.jdesktop.layout.GroupLayout;
import org.w3c.dom.Node;

import javax.swing.*;
import javax.swing.border.TitledBorder;
import javax.swing.border.LineBorder;
import java.awt.*;

/**
 * bug bug: certain methods must be run in the SwingUpdate thread.
 *
 * @author heyes
 */
public class DataNode {

    /** Node being analyzed. */
    private final Node node;

    private final int level;

    /** Value of node. */
    private String value;

    /** JLabel containing attribute name. */
    private JLabel tagField;

    /** JTextField containing attribute value. */
    private JTextField valueField;

    /** If node is not an attribute, it's a panel which contains things. */
    private JPanel container;

    /** Border which may be renamed after constructor called. */
    private TitledBorder titledBorder;

    // We have 4 levels of colored borders
    static private LineBorder[] borders;
    static private Color[] colors;

    static {
        colors    = new Color[4];
        colors[0] = new Color(0,0,255);   // blue
        colors[1] = new Color(155,0,255); // purple
        colors[2] = new Color(180,0,100); // maroon
        colors[3] = new Color(230,0,0);   // red

        borders     = new LineBorder[4];
        borders[0] = new LineBorder(colors[0],1);
        borders[1] = new LineBorder(colors[1],1);
        borders[2] = new LineBorder(colors[2],1);
        borders[3] = new LineBorder(colors[3],1);
    }

    // Layout of the panel
    private GroupLayout layout;
    private GroupLayout.ParallelGroup   hGroup;
    private GroupLayout.SequentialGroup rows;

    
    /**
     * Constructor DataNode creates a new DataNode instance and cleverly
     * stores it in the given Node argument. Thus each node in the the
     * tree of Nodes has a DataNode object stored as user data.
     *
     * @param n of type Node
     * @param level level in node hierarchy (to determine color to use)
     */
    public DataNode(Node n, int level) {
        n.setUserData("DataNode", this, null);   // setUserData(String key, Object data, handler)
        node  = n;
        value = n.getNodeValue();
        this.level = level;
        String pname = n.getNodeName();

        // If node is an XML element's attribute, create a label & value display
        if (n.getNodeType() == Node.ATTRIBUTE_NODE) {
            tagField   = new JLabel(pname);
            valueField = new JTextField(value);
            valueField.setEditable(false);
            valueField.setBackground(Color.white);

        // else if not an attribute, create a panel in which to contain things
        } else {
            container = new JPanel();

            // title on border
            titledBorder = BorderFactory.createTitledBorder(borders[level%4], pname, TitledBorder.LEFT,
                                                            TitledBorder.TOP, null, colors[level%4]);
            container.setBorder(titledBorder);

            // Create layout manager
            layout = new GroupLayout(container);
            container.setLayout(layout);

            // Turn off automatically adding gaps between components
            layout.setAutocreateGaps(false);

            // Turn off automatically creating gaps between components that touch
            // the edge of the container and the container.
            layout.setAutocreateContainerGaps(false);

            // Create a sequential group for the horizontal axis.

            hGroup = layout.createParallelGroup();

            layout.setHorizontalGroup(hGroup);

            rows = layout.createSequentialGroup();

            layout.setVerticalGroup(rows);
        }

    }

    
    /**
     * Add a DataNode object to this DataNode object as part of the JPanel.
     * This method is only used if this object is a container since
     * it doesn't make sense to add things to an attribute
     * (and therefore "layout" is defined).
     *
     * @param dn DataNode object to add
     */
    public void addToPanel(DataNode dn) {
        // if the node we're adding is a container, add the whole container into this one
        if (dn.isContainer()) {
            hGroup.add(dn.getContainer());
            rows.add(dn.getContainer());
        // else if the node we're adding is an attribute, use the layout manager of this
        // object and place in this container
        } else {
            GroupLayout.ParallelGroup   row = layout.createParallelGroup(GroupLayout.BASELINE);
            GroupLayout.SequentialGroup col = layout.createSequentialGroup();
            row.add(dn.getTagField());
            col.add(dn.getTagField());
            row.add(dn.getValueField());
            col.add(dn.getValueField());
            rows.add(row);
            hGroup.add(col);
        }
    }

    /**
     * Remove a DataNode object from this DataNode object as part of the JPanel.
     * This method is only used if this object is a container since
     * it doesn't make sense to remove things from an attribute
     * (and therefore "layout" is defined).
     *
     * @param dn DataNode object to add
     */
    public void removeFromPanel(DataNode dn) {
        // if the node we're removing is a container ...
        if (dn.isContainer()) {
            layout.removeLayoutComponent(dn.getContainer());
        // else if the node we're removing is an attribute ...
        } else {
            layout.removeLayoutComponent(dn.getTagField());
            layout.removeLayoutComponent(dn.getValueField());
        }
    }

    public boolean isContainer() {
        return container != null;
    }

    public int getLevel() {
        return level;
    }

    public JLabel getTagField() {
        return tagField;
    }

    public JTextField getValueField() {
        return valueField;
    }

    public JPanel getContainer() {
        return container;
    }

    /** @return the value */
    public String getValue() {
        return value;
    }

    /** @param value the value to set */
    public void setValue(String value) {
        this.value = value;

        node.setNodeValue(value);
        if (container != null) titledBorder.setTitle(value);
        if (valueField != null) valueField.setText(value);
    }

    /**
     * Method to get the DataNode object associated with a particular Node object.
     *
     * @param n Node object
     * @return assockated DataNode object
     */
    public static DataNode getDataNode(Node n) {
        return (DataNode) n.getUserData("DataNode");
    }

    /**
     * Get the Node object associated with this DataNode object. 
     * @return the Node object associated with this DataNode object
     */
    public Node getNode() {
        return node;
    }

}
