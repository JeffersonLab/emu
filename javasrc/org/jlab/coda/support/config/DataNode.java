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

package org.jlab.coda.support.config;

import org.w3c.dom.Node;

import javax.swing.*;
import java.awt.*;

/** @author heyes */
public class DataNode extends JPanel {
    /** Field node */
    private final Node node;
    /** Field value */
    private String value = null;

    /** Field valueField */
    private JTextField valueField;

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

            this.setLayout(new BorderLayout());

            JLabel tagField;
            if (!pname.matches("value")) {
                tagField = new JLabel(pname);
            } else {
                tagField = new JLabel("");
            }
            //tagField.setEditable(false);

            this.add(tagField, BorderLayout.LINE_START);

            valueField = new JTextField(value);

            this.add(valueField, BorderLayout.CENTER);

        } else if (pname.matches("component")) {

            this.setLayout(new BoxLayout(this, BoxLayout.LINE_AXIS));
            this.setAlignmentY(JPanel.TOP_ALIGNMENT);
            this.setBorder(BorderFactory.createTitledBorder(pname));

        } else {

            this.setLayout(new BoxLayout(this, BoxLayout.PAGE_AXIS));

            this.setAlignmentX(JPanel.LEFT_ALIGNMENT);
            this.setBorder(BorderFactory.createTitledBorder(pname));
        }

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
