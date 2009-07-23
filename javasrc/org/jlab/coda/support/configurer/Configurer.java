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

import org.jlab.coda.support.logger.Logger;
import org.w3c.dom.*;
import org.w3c.dom.bootstrap.DOMImplementationRegistry;
import org.w3c.dom.ls.*;
import org.w3c.dom.traversal.NodeFilter;

/**
 * This class is a singleton and contains one static reference
 * to a DOM builder and a DOM serializer. It is used to
 * configure the EMU using XML from a file or string.<p>
 * 
 * <code>LSParserFilter</code>s provide applications the ability to examine
 * nodes as they are being constructed while parsing. As each node is
 * examined, it may be modified or removed, or the entire parse may be
 * terminated early.
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

    /** DOMImplementationLS contains the factory methods for creating
     * Load and Save objects (objects that can parse and serialize XML DOM documents).
     */
    private static DOMImplementationLS domImplementor;

    /** Object that can create a DOM tree. */
    private static LSParser domBuilder;

    /** Object for serializing (writing) a DOM document out into XML. */
    private static LSSerializer domWriter;

    /** Field configureMode */
    protected static int configureMode = 0;

    static {
        try {
            // create Error Handler (an item of this class)
            DOMErrorHandler errorHandler = new Configurer();
            
            // get DOM Implementation using DOM Registry
            // bug bug: use default SUN implementation instead?
            //System.setProperty(DOMImplementationRegistry.PROPERTY, "org.apache.xerces.dom.DOMXSImplementationSourceImpl");
            DOMImplementationRegistry registry = DOMImplementationRegistry.newInstance();

            // get one object that can create XML DOM parsers & serializers
            domImplementor = (DOMImplementationLS) registry.getDOMImplementation("LS");

            // create one object that can create a DOM tree
            domBuilder = domImplementor.createLSParser(DOMImplementationLS.MODE_SYNCHRONOUS, null);

            // get object used to configure the DOM parser
            DOMConfiguration config = domBuilder.getDomConfig();

            // create filter (an item of this class) and add it to the parser
            // bug bug: why do we have 2 objects of this class being created?
            // What's the point of making this object an LSParserFilter?
            // All that is done below is to show every node to the filter and
            // then accept everything. Were there some plans to filter nodes?
            LSParserFilter filter = new Configurer();
            domBuilder.setFilter(filter);

            // set error handler in DOM parser
            config.setParameter("error-handler", errorHandler);

            // set validation feature in DOM parser
            config.setParameter("validate", Boolean.FALSE);

            // create the DOM serializer
            domWriter = domImplementor.createLSSerializer();

            // get object used to configure the DOM serializer
            config = domWriter.getDomConfig();
            config.setParameter("xml-declaration", Boolean.FALSE);

        } catch (Exception e) {
            //System.out.println("Exception initializing class Configurer : " + e.getMessage());
        }
    }


    /**
     * Remove any empty text (child) nodes from a DOM node.
     * @param node node from which to remove empty (child) text nodes
     */
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
     * Method to parse a file containing an XML configuration.
     *
     * @param configFile file containing an XML configuration
     * @return Document
     * @throws DataNotFoundException when
     */
    public static Document parseFile(String configFile) throws DataNotFoundException {
        try {
            // parse document
            return domBuilder.parseURI(configFile);
        } catch (Exception e) {
            throw new DataNotFoundException("Cannot parse configuration file", e);
        }

    }

    /**
     * Method to parse a string containing an XML configuration.
     *
     * @param xmlConfig string containing an XML configuration
     * @return Document
     * @throws DataNotFoundException when
     */
    public static Document parseString(String xmlConfig) throws DataNotFoundException {

        LSInput input = domImplementor.createLSInput();
        input.setStringData(xmlConfig);
        try {
            // parse document
            return domBuilder.parse(input);
        } catch (Exception e) {
            throw new DataNotFoundException("Cannot parse XML string", e);
        }
    }


    /**
     * Method to serialize a DOM XML document into a string.
     *
     * @param doc DOM object to serialize to string
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
     * Method to handle errors.
     *
     * @param error error of type DOMError
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
        // bug bug: why is this not LSParseFilter.FILTER_ACCEPT ?  - they're identical
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

    // bug bug: getValue isn't symmetric with setValue, but then it is never used
    /**
     * Method to get the value of the Node object given by the path argument or
     * null if there is no such object. If the Node is an attribute it's value
     * is returned, else if <b>not</b> an attribute, then it returns it's first
     * child's text (or null if no kids).
     *
     * @param doc  DOM XML Document object
     * @param path path into the XML object
     * @return value of the Node associated with the given path
     * @throws DataNotFoundException never thrown
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
     * Method to get the DataNode object associated with the
     * Node object given by the path argument.
     *
     * @param doc  DOM XML Document object
     * @param path path into the XML object
     * @return DataNode object associated with the given path,
     *         null if Node does exist at the specified path
     * @throws DataNotFoundException when no DataNode object associated with the Node at the given path
     */
    private static DataNode getData(Document doc, String path) throws DataNotFoundException {
        Node n = getNode(doc, path);
        if (n == null) return null;
        DataNode dn = (DataNode) n.getUserData("DataNode");
        if (dn == null) throw new DataNotFoundException("no DataNode associated with path " + path);
        return dn;
    }

    /**
     * Method to set the value of the DataNode object assocated with the
     * Node object given by the path argument. Does nothing if there is no
     * DataNode object at the given path.
     *
     * @param doc  DOM XML Document object
     * @param path path into the XML object
     * @param value value to set the DataNode object to
     * @throws DataNotFoundException when
     */
    public static void setValue(Document doc, String path, String value) throws DataNotFoundException {
        DataNode dn = getData(doc, path);
        if (dn == null) return;
        dn.setValue(value);
    }

    /**
     * Method to set the value of the DataNode object assocated with the
     * Node object given by the path argument. Does nothing if there is no
     * DataNode object at the given path.<p>
     * Not Used.
     *
     * @param doc  DOM XML Document object
     * @param path path into the XML object
     * @param name name of new attribute to add
     * @param value value of the new attribute
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
     * Method to set the value of the DataNode object assocated with the
     * Node object given by the path argument. Does nothing if there is no
     * DataNode object at the given path.<p>
     * Not Used.
     *
     * @param doc  of type Document
     * @param path of type String
     * @param name of type String
     * @throws DataNotFoundException when
     */
    public static void newContainer(Document doc, String path, String name) throws DataNotFoundException {
        Node n = getNode(doc, path);
        if (n == null) return;

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
                Node el = n;

                NamedNodeMap nnm = el.getAttributes();
                //System.out.println("Looking for " + value + " in " +n);
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
