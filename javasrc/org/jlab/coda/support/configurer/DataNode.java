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

/** @author heyes */
public class DataNode {
    /** Field node */
    private final Node node;
    /** Field value */
    private String value = null;

    private JLabel tagField = null;
    /** Field valueField */
    private JTextField valueField = null;

    private JPanel container = null;

    private GroupLayout layout;
    private GroupLayout.ParallelGroup hGroup;

    private GroupLayout.SequentialGroup rows;

    /**
     * Constructor DataNode creates a new DataNode instance.
     *
     * @param n of type Node
     */
    public DataNode(Node n) {
        n.setUserData("DataNode", this, null);
        node = n;
        String pname = n.getNodeName();

        value = n.getNodeValue();

        if (n.getNodeType() == Node.ATTRIBUTE_NODE) {
            tagField = new JLabel(pname);
            valueField = new JTextField(value);
        } else {
            container = new JPanel();
            container.setBorder(BorderFactory.createTitledBorder(pname));

            layout = new GroupLayout(container);
            container.setLayout(layout);
            // Turn on automatically adding gaps between components
            layout.setAutocreateGaps(false);

            // Turn on automatically creating gaps between components that touch
            // the edge of the container and the container.
            layout.setAutocreateContainerGaps(false);

            // Create a sequential group for the horizontal axis.

            hGroup = layout.createParallelGroup();

            layout.setHorizontalGroup(hGroup);

            rows = layout.createSequentialGroup();

            layout.setVerticalGroup(rows);

        }

    }

    public void add(DataNode dn) {
        if (dn.isContainer()) {
            hGroup.add(dn.getContainer());
            rows.add(dn.getContainer());
        } else {
            GroupLayout.ParallelGroup row = layout.createParallelGroup(GroupLayout.BASELINE);
            GroupLayout.SequentialGroup col = layout.createSequentialGroup();
            row.add(dn.getTagField());
            col.add(dn.getTagField());
            row.add(dn.getValueField());
            col.add(dn.getValueField());
            rows.add(row);
            hGroup.add(col);
        }
    }

    public boolean isContainer() {
        return container != null;
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
        valueField.setText(value);
    }

    /**
     * Method getDataNode ...
     *
     * @param n of type Node
     * @return DataNode
     */
    public static DataNode getDataNode(Node n) {
        return (DataNode) n.getUserData("DataNode");
    }

    /** @return the parent */
    public Node getNode() {
        return node;
    }

}
