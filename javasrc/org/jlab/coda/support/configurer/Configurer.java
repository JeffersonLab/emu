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

/**
 * Configure the EMU using XML from a file or string.
 */

package org.jlab.coda.support.configurer;

import org.jlab.coda.support.logger.Logger;
import org.w3c.dom.*;
import org.w3c.dom.bootstrap.DOMImplementationRegistry;
import org.w3c.dom.ls.*;
import org.w3c.dom.traversal.NodeFilter;

/**
 * -----------------------------------------------------
 * Copyright (c) 2008 Jefferson lab data acquisition group
 * Class Configurer ...
 *
 * @author heyes
 *         Created on Sep 12, 2008
 */
public class Configurer implements DOMErrorHandler, LSParserFilter {

    /** Default namespaces support (true). */
    protected static final boolean DEFAULT_NAMESPACES = true;

    /** Default validation support (false). */
    protected static final boolean DEFAULT_VALIDATION = false;

    /** Default Schema validation support (false). */
    protected static final boolean DEFAULT_SCHEMA_VALIDATION = false;

    /** Field DEFAULT_PARSE_MODE */
    protected static final int DEFAULT_PARSE_MODE = 0;
    /** Field STRING_PARSE_MODE */
    protected static final int STRING_PARSE_MODE = 1;
    /** Field FILE_PARSE_MODE */
    protected static final int FILE_PARSE_MODE = 2;

    /** Field impl */
    private static DOMImplementationLS impl;

    /** Field builder */
    private static LSParser builder;

    /** Field domWriter */
    private static LSSerializer domWriter;

    /** Field configureMode */
    protected static int configureMode = 0;

    static {
        try {
            // create Error Handler
            DOMErrorHandler errorHandler = new Configurer();
            // get DOM Implementation using DOM Registry
            System.setProperty(DOMImplementationRegistry.PROPERTY, "org.apache.xerces.dom.DOMXSImplementationSourceImpl");
            DOMImplementationRegistry registry = DOMImplementationRegistry.newInstance();

            impl = (DOMImplementationLS) registry.getDOMImplementation("LS");

            // create DOMBuilder
            builder = impl.createLSParser(DOMImplementationLS.MODE_SYNCHRONOUS, null);
            DOMConfiguration config = builder.getDomConfig();

            // create filter
            LSParserFilter filter = new Configurer();

            builder.setFilter(filter);

            // set error handler
            config.setParameter("error-handler", errorHandler);

            // set validation feature
            config.setParameter("validate", Boolean.FALSE);

            domWriter = impl.createLSSerializer();

            config = domWriter.getDomConfig();
            config.setParameter("xml-declaration", Boolean.FALSE);

        } catch (Exception e) {
            //System.out.println("Exception initializing class Configurer : " + e.getMessage());
        }
    }

    public static void removeEmptyTextNodes(Element node) {
        Node el = node.getFirstChild();
        while (el != null) {
            Node next = el.getNextSibling();
            switch (el.getNodeType()) {
                case Node.TEXT_NODE:
                    String str = el.getNodeValue().trim();
                    if (str.equals("")) {
                        node.removeChild(el);
                    }
                    break;
                case Node.ELEMENT_NODE:
                    removeEmptyTextNodes((Element) el);
                    break;
                case Node.COMMENT_NODE:
                    node.removeChild(el);
                    break;
            }
            el = next;
        }
    }

    /**
     * Method parseFile ...
     *
     * @param configFile of type String
     * @return Document
     * @throws DataNotFoundException when
     */
    public static Document parseFile(String configFile) throws DataNotFoundException {
        try {
            // parse document
            return builder.parseURI(configFile);
        } catch (Exception e) {
            throw new DataNotFoundException(e.getMessage());
        }

    }

    /**
     * Method parseString ...
     *
     * @param xmlConfig of type String
     * @return Document
     * @throws DataNotFoundException when
     */
    public static Document parseString(String xmlConfig) throws DataNotFoundException {

        LSInput input = impl.createLSInput();
        input.setStringData(xmlConfig);
        try {
            // parse document
            return builder.parse(input);
        } catch (Exception e) {
            throw new DataNotFoundException(e.getMessage());
        }
    }

    /**
     * Method serialize ...
     *
     * @param doc of type Document
     * @return String
     */
    public static String serialize(Document doc) {
        try {
            return domWriter.writeToString(doc);
        } catch (Exception ex) {
            ex.printStackTrace();
            return null;
        }
    }

    /**
     * Method handleError ...
     *
     * @param error of type DOMError
     * @return boolean
     */
    public boolean handleError(DOMError error) {
        short severity = error.getSeverity();
        if (severity == DOMError.SEVERITY_ERROR) {
            Logger.error("[dom3-error]: " + error.getMessage());
        }

        if (severity == DOMError.SEVERITY_WARNING) {
            Logger.error("[dom3-warning]: " + error.getMessage());
        }
        return true;

    }

    /** @see org.w3c.dom.ls.LSParserFilter#acceptNode(Node) */
    public short acceptNode(Node enode) {
        return NodeFilter.FILTER_ACCEPT;
    }

    /** @see org.w3c.dom.ls.LSParserFilter#getWhatToShow() */
    public int getWhatToShow() {
        return NodeFilter.SHOW_ELEMENT;
    }

    /** @see org.w3c.dom.ls.LSParserFilter#startElement(Element) */
    public short startElement(Element elt) {
        return LSParserFilter.FILTER_ACCEPT;
    }

    /**
     * Method getValue ...
     *
     * @param doc  of type Document
     * @param path of type String
     * @return String
     * @throws DataNotFoundException when
     */
    public static String getValue(Document doc, String path) throws DataNotFoundException {

        Node n = getNode(doc, path);

        if (n == null) return null;

        if (n.getNodeType() == Node.ATTRIBUTE_NODE) {
            return n.getNodeValue();
        } else {
            if (n.getFirstChild() != null) return n.getFirstChild().getTextContent();
            else return null;
        }
    }

    /**
     * Method getData ...
     *
     * @param doc  of type Document
     * @param path of type String
     * @return DataNode
     * @throws DataNotFoundException when
     */
    private static DataNode getData(Document doc, String path) throws DataNotFoundException {
        Node n = getNode(doc, path);
        if (n == null) return null;
        DataNode dn = (DataNode) n.getUserData("DataNode");
        if (dn == null) throw new DataNotFoundException("no DataNode associated with path " + path);
        return dn;
    }

    /**
     * Method setValue ...
     *
     * @param doc   of type Document
     * @param path  of type String
     * @param value of type String
     * @throws DataNotFoundException when
     */
    public static void setValue(Document doc, String path, String value) throws DataNotFoundException {

        DataNode dn = getData(doc, path);
        if (dn == null) return;
        dn.setValue(value);
    }

    /**
     * Method newValue ...
     *
     * @param doc   of type Document
     * @param path  of type String
     * @param name  of type String
     * @param value of type String
     * @throws DataNotFoundException when
     */
    public static void newValue(Document doc, String path, String name, String value) throws DataNotFoundException {
        Node n = getNode(doc, path);
        if (n == null) return;

        Element el = (Element) n;

        Attr a = doc.createAttribute(name);
        a.setNodeValue(value);

        el.setAttributeNode(a);

        DataNode newdn = new DataNode(a);

        DataNode dn = (DataNode) n.getUserData("DataNode");

        if (dn != null) dn.add(newdn);

    }

    /**
     * Method newContainer ...
     *
     * @param doc  of type Document
     * @param path of type String
     * @param name of type String
     * @throws DataNotFoundException when
     */
    public static void newContainer(Document doc, String path, String name) throws DataNotFoundException {
        Node n = getNode(doc, path);

        Node newNode = doc.createElement(name);

        n.appendChild(newNode);

        DataNode newdn = new DataNode(newNode);

        DataNode dn = (DataNode) n.getUserData("DataNode");

        if (dn != null) dn.add(newdn);

    }

    public static DataNode getDataNodes(Node node) {

        DataNode dn = new DataNode(node);

        if (node.hasChildNodes()) {
            NodeList l = node.getChildNodes();
            for (int jx = 0; jx < l.getLength(); jx++) {
                Node n = l.item(jx);
                String nn = n.getNodeName();
                if ((nn != null) && !nn.startsWith("#")) {
                    if (nn.matches("name")) {
                        // special case
                        if (n.hasAttributes()) {
                            NamedNodeMap attr = n.getAttributes();
                            Node nameAttr = attr.getNamedItem("name");
                            if (nameAttr != null) dn.setValue(nameAttr.getNodeValue());
                            // model.setValueAt(nameAttr.getNodeValue(),titleRow,col);
                        } else {
                            // model.setValueAt(n.getTextContent(),titleRow,col);
                            dn.add(new DataNode(n));
                        }

                    } else {
                        dn.add(getDataNodes(n));
                    }
                }

            }
        }

        if (node.hasAttributes()) {
            NamedNodeMap attr = node.getAttributes();

            for (int ix = 0; ix < attr.getLength(); ix++) {
                Node aNode = attr.item(ix);
                DataNode adn = new DataNode(aNode);
                dn.add(adn);
            }
        }
        // dn.add(Box.createHorizontalGlue());
        return dn;
    }

    /**
     * Method getNode ...
     *
     * @param doc  of type Document
     * @param path of type String
     * @return Node
     * @throws DataNotFoundException when
     */
    public static Node getNode(Document doc, String path) throws DataNotFoundException {
        String[] s = path.split("/");
        Node n = doc;
        if (n == null) return null;
        NodeList l = n.getChildNodes();
        Node found = null;
        for (String value : s) {
            found = null;
            // search for a child with name s[ix]
            for (int jx = 0; jx < l.getLength(); jx++) {
                String nm = l.item(jx).getNodeName();
                if (nm != null) {
                    if (nm.matches(value)) {
                        found = l.item(jx);
                        break;
                    }
                }
            }

            if (found == null) {
                // could be an attribute so look for attribute named s[ix]
                Element el = (Element) n;

                NamedNodeMap nnm = el.getAttributes();

                found = nnm.getNamedItem(value);
                if (found != null) {
                    return found;
                }
            }

            if (found == null) {
                throw new DataNotFoundException("No child node of " + n.getNodeName() + " named " + value);
            }

            n = found;
            l = n.getChildNodes();
        }
        if (found != null) return found;
        else throw new DataNotFoundException("No node found with path : " + path);
    }

}
