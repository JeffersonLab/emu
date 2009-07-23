/*----------------------------------------------------------------------------
 *  Copyright (c) 2007        Southeastern Universities Research Association, 
 *                            Thomas Jefferson National Accelerator Facility  
 *                                                                            
 *    This software was developed under a United States Government license    
 *    described in the NOTICE file included as part of this distribution.     
 *                                                                            
 *    Author:  heyes
 *    Created: Oct 23, 2007                      
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
 *      emu  - Data.h
 *
 *----------------------------------------------------------------------------*/


#ifndef DATA_H_
#define DATA_H_

#include <string>
#include <map>
#include <vector>

using namespace std;

class Data {
    
public:
    // The root of the tree
    /** Objects associate to form a tree with one root. The metadata is assumed to be organized
     * in a tree structure with one root and many branches. Since there is only
     * one root it is implemented as a static member of the class.
     */
    static Data root;
    	
    // Creation and distruction
    /** Constructor, no-arg. */
    Data();
    /** More useful constructor object has name, value and is associated with a container. */
    Data(Data *owner, char *pname,char *pvalue);
    /** Destructor. */
    ~Data();
    /** Does nothing except call clear. */
    void clear();
    
    // Object management
    /** Sets the name of this tree node. */
    void setName(char *pname);
    /** Gets the name of this tree node. */
    const char *getName();
    /** Sets the family of this tree node. An object can be associated with a family of similar objects. */
    void setFamily(char *ptag);
    /** Gets the family of this tree node. */
    const char *getFamily();
    /** Sets the unnamed attribute called "value".
     * Even though the object has named attributes this is one that is unnamed. */
    void setValue(char *pvalue);
    /** Gets the attribute called "value". */
    const char *getValue();
    /** Gets the string representing the path from the root of the data tree to this object. */
    string getPath();
    /** Sets the container that this object is in (also sets the path). */
    void setOwner(Data *parent);
    /** Adds a child Data object to this node. A Data object is a container for other Data objects. */
    void addData(Data *child);
    /** Adds an attribute to this node. A Data object can have many tag+value attributes. */
    void addAttr(char *tag,char *value);
    /** Gets an attribute's value given its name. */
    char *getAttr(char *name); 

    // Searching
    // These functions search the tree starting at this object. 
    // Use with root to search the whole tree.
    
    /** Given a path, return the value of the first object found. */
    const char *findValueByPath(string searchpath);
    /** Given a path, return the Data object at that location */
    Data *findOneByPath(string searchpath);
    /** Given a path, return all Data objects (used with wildcard *). */
    vector<Data*> *findAllByPath(string searchpath);
    
    /** Given a name, return the value of the first object found. */
    const char *findValueByName(string searchpath);
    /** Given a name, findOneByPath the first object with that name. */
    Data *findOneByName(string cname);
    /** Given a name, findOneByPath all objects with that name. */
    vector<Data*> *findAllByName(string cname);
    
    /** Given a family, return the value of the first object found. */
    const char *findValueByFamily(string searchpath);
    /** Given a family, return the first object found in that family. */
    Data *findOneByFamily(string cname);
    /** Given a family, return the all objects in that family. */
    vector<Data*> *findAllByFamily(string cname);

    /** Represent this object as a string. */
    char *serialize(int cont);
    
private:
    
    /** Used to keep the printout pretty. */
    void serialize(int indent,string *s,int cont);
    /** Used internally by findOneByPath. */
    void findAll(vector<Data*> *v,string path);
    
    /** Name of this object. */
    string name;
    /** Path to this object from root. */
    string path;
    /** Family of objects similar to this one. */
    string family;
    /** Value, default unnamed attribute. */
    string value;
    /** Container this object is in. */
    Data *owner;

    /** Objects contained by this object. */
    vector<Data*> owned;
    /** Tag/Value attributes of this object. */
    map<string,string> attr;
};

#endif /*DATA_H_*/
