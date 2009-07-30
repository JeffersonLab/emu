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
            // create one item of this class
            Configurer instance = new Configurer();

            // create Error Handler (an item of this class)
            DOMErrorHandler errorHandler = instance;

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
            LSParserFilter filter = instance;
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
     * Method to add an attribute (name/value pair) DataNode object
     * to or replace one with the same name assocated with the
     * Node object given by the path argument. Does nothing if
     * there is no Node object at the given path.<p>
     * Not Used.
     *
     * @param doc  DOM XML Document object
     * @param path path into the XML object
     * @param name name of new attribute to add
     * @param value value of the new attribute
     * @throws DataNotFoundException when
     */
    public static void newValue(Document doc, String path, String name, String value)
            throws DataNotFoundException {
        Node n = getNode(doc, path);
        if (n == null) return;

        // the Element class extends Node
        Element el = (Element) n;

        // the Attr class extends Node
        Attr a = doc.createAttribute(name);
        a.setNodeValue(value);

        // add this attribute node to tree or replace one with the same name
        Attr replacedNode = el.setAttributeNode(a);

        // now the GUI stuff
        DataNode dn = (DataNode) n.getUserData("DataNode");

        // if there is a GUI ...
        if (dn != null) {
            // if we're replacing an existing node, remove the old one from the GUI first
            // carl added this (is it necessary?)
            if (replacedNode != null) {
System.out.println("Attribute node being replaced");
                DataNode rdn = (DataNode) replacedNode.getUserData("DataNode");
                if (rdn != null) dn.removeFromPanel(rdn);
            }

            // create a DataNode object out of it so it can be added to GUI
            DataNode newdn = new DataNode(a, dn.getLevel()+1);

            // add to GUI
            dn.addToPanel(newdn);
        }
    }


    /**
     * Method to add a new container DataNode object to the
     * Node object given by the path argument. Does nothing if
     * there is no Node object at the given path.<p>
     * Not Used.
     *
     * @param doc  DOM XML Document object
     * @param path path into the XML object
     * @param name name of new container to add
     * @throws DataNotFoundException when
     */
    public static void newContainer(Document doc, String path, String name)
            throws DataNotFoundException {
        Node n = getNode(doc, path);
        if (n == null) return;

        // add new node to tree
        Node newNode = doc.createElement(name);
        n.appendChild(newNode);

        // add to GUI
        DataNode dn = (DataNode) n.getUserData("DataNode");
        if (dn != null) {
            DataNode newdn = new DataNode(newNode, dn.getLevel()+1);
            dn.addToPanel(newdn);
        }
    }


    /**
     * Recursive method to, using the given arg as the top of the tree, add all nodes
     * in the tree to a JPanel for display in a GUI.<p>
     * (Formerly method named getDataNodes.)
     *
     * @param node Node object to be displayed in a JPanel
     * @param level level of node in XML document tree (0 = top)
     * @return DataNode object associated with arg
     */
    public static DataNode treeToPanel(Node node, int level) {

        DataNode dn = new DataNode(node, level);

        // Set name of this panel. Default is node name from parsing XML,
        // but use the "name" attribute if it's defined.
        if (node.hasAttributes()) {
            NamedNodeMap attr = node.getAttributes();
            Node nameAttr = attr.getNamedItem("name");
            if (nameAttr != null) {
//System.out.println("treeToPanel:   name = " + nameAttr.getNodeValue());
                dn.setValue(nameAttr.getNodeValue());
            }
            else {
                // do this to get Title in JInternalFrame
                dn.setValue(node.getNodeName());
            }
        }
        else {
            // do this to get Title in JInternalFrame
            dn.setValue(node.getNodeName());            
        }

        // each meaningful child is added to the panel
        if (node.hasChildNodes()) {
            // for each child of node ...
            NodeList l = node.getChildNodes();
            for (int jx = 0; jx < l.getLength(); jx++) {
                Node n = l.item(jx);
                String nn = n.getNodeName();

                // ignore child node who has no name or whose name start with "#" (comment)
                if ((nn != null) && !nn.startsWith("#")) {
//System.out.println("treeToPanel: add " + nn);
                    dn.addToPanel(treeToPanel(n, level+1));
                }
            }
        }

        // each attribute is added to the panel
        if (node.hasAttributes()) {
            NamedNodeMap attr = node.getAttributes();
            for (int ix = 0; ix < attr.getLength(); ix++) {
                Node aNode = attr.item(ix);
                DataNode adn = new DataNode(aNode, level+1);
                dn.addToPanel(adn);
            }
        }

        return dn;
    }



/**
     * Recursive method to, using the given arg as the top of the tree, add all nodes
     * in the tree to a JPanel for display in a GUI.<p>
     * Formerly method named getDataNodes.
     *
     * @param node Node object to be displayed in a JPanel
     * @return DataNode object associated with arg
     */
/*
    public static DataNode treeToPanel2(Node node) {

        DataNode dn = new DataNode(node);

        if (node.hasChildNodes()) {
            NodeList l = node.getChildNodes();

            // for each child of node ...
            for (int jx = 0; jx < l.getLength(); jx++) {
                Node n = l.item(jx);
                String nn = n.getNodeName();
System.out.println("treeToPanel: child name = " + nn);

                // ignore child node who has no name or whose name start with "#" (comment)
                if ((nn != null) && !nn.startsWith("#")) {
                    // if the child node has attributes, look for one called "name"
                    // since that will now be the label for its panel
                    if (n.hasAttributes()) {
 System.out.println("treeToPanel: child has attributes");
                        NamedNodeMap attr = n.getAttributes();
                        Node nameAttr = attr.getNamedItem("name");
                        if (nameAttr != null) {
System.out.println("treeToPanel: child's \"name\" attribute name is " + nameAttr.getNodeValue());
                            dn.setValue(nameAttr.getNodeValue());
                        }

                        // model.setValueAt(nameAttr.getNodeValue(),titleRow,col);
                        // else if child node has NO attributes (empty container or attribute ??),
                        // add only it (and not tree under it) to the top panel
                        // model.setValueAt(n.getTextContent(),titleRow,col);
                        System.out.println("treeToPanel: \"name\" child has no attributes");
                        dn.addToPanel(new DataNode(n));
                        // else if child node's name is NOT "name",
                    // add the whole branch to top panel
                    } else {
                        dn.addToPanel(treeToPanel(n));
                    }
                }
            }
        }

        // add all attriubtes directly to the panel
        if (node.hasAttributes()) {
            NamedNodeMap attr = node.getAttributes();

            for (int ix = 0; ix < attr.getLength(); ix++) {
                Node aNode = attr.item(ix);
                DataNode adn = new DataNode(aNode);
                dn.addToPanel(adn);
            }
        }
        // dn.add(Box.createHorizontalGlue());
        return dn;
    }
*/


    /**
     * Method to get the Node object associated with a specific path
     * into an XML document.
     *
     * @param doc  DOM XML Document object
     * @param path path into the XML object
     * @return Node object (or null if none)
     * @throws DataNotFoundException when
     */
    public static Node getNode(Document doc, String path) throws DataNotFoundException {
        String[] s = path.split("/");
        Node n = doc;
        if (n == null) return null;

        NodeList childNodes = n.getChildNodes();
        Node found = null;

        // for each part of the path ...
        for (String partialPath : s)  {
            found = null;

            // search for a child with same name as partialPath
            for (int jx = 0; jx < childNodes.getLength(); jx++) {
                String childName = childNodes.item(jx).getNodeName();
                if (childName != null) {
                    System.out.println("getNode: looking in node " + childName);
                }
                if (childName != null && childName.matches(partialPath)) {
                    found = childNodes.item(jx);
                    break;
                }
            }

            // if we have NOT found the partialPath among the child nodes ...
            if (found == null) {
                // it could be an attribute so look for attribute named partialPath
                Node el = n;

                // returns null if not an Element object
                NamedNodeMap nnm = el.getAttributes();
                if (nnm != null) {
                    System.out.println("getNode: Looking for " + partialPath + " in " + n + ", attri = " + nnm);
                    found = nnm.getNamedItem(partialPath);
                    // if we've found an attribute, it's the end of the line
                    if (found != null) {
                        return found;
                    }
                }
                else {
                    System.out.println("getNode: cannot find " + partialPath + " in nodes or attributes");
                }
            }

            if (found == null) {
                throw new DataNotFoundException("No child node of " + n.getNodeName() + " named " + partialPath);
            }

            // now try to match up the next round of children with the next part ot the path
            n = found;
            childNodes = n.getChildNodes();
        }

        if (found != null) {
            return found;
        }
        else {
            throw new DataNotFoundException("No node found with path : " + path);
        }
    }

}
