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
    static Data root;								// Objects associate to form a tree with one root
    		
	// Creation and distruction
    Data();											// Constructor, nothing assigned at construction time
    Data(Data *owner, char *pname,char *pvalue);	// More useful constructor object has name, value and is associated with a container
    ~Data();										// Destructor
    void clear();									// Does nothing except call clear
    
    // Object management
    void setName(char *pname);						// Set the name					
    const char *getName();							// Get the name
    void setFamily(char *ptag);						// An object can be associated with a family of similar objects
    const char *getFamily();						// Get the family
    void setValue(char *pvalue);					// Even though the object has nemed attributes it has an unnamed
    const char *getValue();							// Attribute caled the value  
    string getPath();								// String representing the path from the root of the data tree to this object
    void setOwner(Data *parent);					// Set the container that this object is in (also sets the path)
    void addData(Data *child);						// A Data object is a container for other Data objects
    void addAttr(char *tag,char *value);			// A Data object can have tag+value attributes
    char *getAttr(char *name);						// Given it's name return an attribute value

    // Searching
    // These functions search the tree starting at this object. 
    // Use with root to search the whole tree.
    
    const char *findValueByPath(string searchpath);			// Given a path return the value of the first object found
    Data *findOneByPath(string searchpath);					// Given a path return the Data object at that location
    vector<Data*> *findAllByPath(string searchpath);		// Given a path return all Data objects (used with wildcard *) 
  
    const char *findValueByName(string searchpath);			// Given a name return the value of the first object found  
    Data *findOneByName(string cname);						// Given a name findOneByPath the first object with that name 
    vector<Data*> *findAllByName(string cname);				// Given a name findOneByPath all objects with that name
    
    const char *findValueByFamily(string searchpath);		// Given a family return the value of the first object found
    Data *findOneByFamily(string cname);					// Given a family return the first object found in that family
    vector<Data*> *findAllByFamily(string cname);			// Given a family return the all objects in that family

    char *serialize(int cont);								// Represent this object as a string
    
private:
    void serialize(int indent,string *s,int cont);			// Use to keep the printout pretty	
    void findAll(vector<Data*> *v,string path);				// Used internally by findOneByPath
    
    string name; 				// Name of this object
    string path;				// Path to this object from root
    string family;				// Family of objects similar to this one
    string value;				// Value, default unnamed attribute
    Data *owner;				// Container this object is in

    vector<Data*> owned;		// Objects contained by this object
    map<string,string> attr;	// tag+value attributes of this object
};

#endif /*DATA_H_*/
