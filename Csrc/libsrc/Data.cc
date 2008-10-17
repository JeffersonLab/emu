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
 *      emu  - Data.cpp 
 * This class represents metadata objects.Each metadata object has a name,
 * family (type), value and attributes. The metadata is assumed to be organized 
 * in a tree structure with one root and many branches. Since there is only 
 * one root it is implemented as a static member of the class.
 * Each Data object can be a branch point in the tree and therfore has one 
 * owner and several owned. Mixed metaphors trees and owned
 * TODO : unmix the metaphors.
 *----------------------------------------------------------------------------*/

#include <string>
#include <map>
#include <vector>

using namespace std;

#include "Data.hxx"

// Root of the metadata tree. Static member of class Data
Data Data::root;

/* Data(owner, name,value) - constructor
 *
 * Construct a data object
 * 
 * @param Data* owner - 
 * @param char* name - name of this Data object 
 * @param char* value - value assigned to this data object
 *
 * @return
 * 

 */

Data::Data(Data *powner, char *pname,char *pvalue) {
    name = pname;
    value = pvalue;
    owner = powner;
    path.clear();
    if(owner != NULL) {
        path.append(owner->path);
        path.append("/");
        path.append(owner->name);
    } else {
        path.append("");
    }
}

/*
 *
 * clear()
 * 
 * Delete all Data objects contained by this object
 *
 * @param 
 *
 * @return
 * 

 */

void Data::clear() {
    vector<Data *>::iterator ix;

    for (ix= owned.begin();ix != owned.end();ix++) {

        Data *c = *ix;
        delete c;
    }

    attr.clear();
    owned.clear();
}

/*
 * findValueByPath(searchpath)
 * 
 * Given a unique path return the value of the object found there.
 *
 * @param string path - place to look
 *
 * @return char * - value of matching object
 */

const char *Data::findValueByPath(string searchpath) {
    Data *found = findOneByPath(searchpath);
    if (found == NULL)
        return NULL;
    else
        return found->getValue();
}

/*
 * findOneByPath(path)
 * 
 * Given a path return the matching object.
 *
 * @param string searchpath - place to look
 *
 * @return *Data - matching object
 */

Data *Data::findOneByPath(string searchpath) {
    vector<Data *>::iterator ix;
    string finder;

    for (ix= owned.begin();ix != owned.end();ix++) {
        finder.clear();
        finder.append((*ix)->getPath());
        if (searchpath.compare(searchpath.length()-2,2,"/*") == 0) {
            finder.append("/*");
        } else {
            finder.append("/");
            finder.append((*ix)->getName());
        }

        if ((searchpath.compare(finder) == 0)) {
            return (*ix);
        } else {
            Data *found = (*ix)->findOneByPath(searchpath);
            if (found != NULL)
                return found;
        }
    }

    return NULL;
}

/*
 * findAllByPath(string searchpath)
 * 
 * Given a path containing a wildcard return a vector of
 * matching objects
 *
 * @param string searchpath - place to look
 *
 * @return *vector<Data*> - matching objects
 */

vector<Data*> *Data::findAllByPath(string searchpath) {
    vector<Data*> *v = new vector<Data*>();
    v->clear();
    Data::root.findAll(v,searchpath);
    return v;
}

// Same as above but searching using name

const char *Data::findValueByName(string pname) {
    Data *found = findOneByName(pname);
    if (found == NULL)
        return NULL;
    else
        return found->getValue();
}

Data *Data::findOneByName(string cname) {
    vector<Data *>::iterator ix;

    for (ix= owned.begin();ix != owned.end();ix++) {
        Data *child = *ix;
        if (cname.compare(child->getName()) == 0) {
            return child;
        }
    }

    return NULL;
}

vector<Data*> *Data::findAllByName(string cname) {
    string::size_type loc = cname.find( '/', 0 );
    if( loc != string::npos ) {
        return findAllByPath(cname);
    }
    vector<Data *>::iterator ix;
    vector<Data*> *v = new vector<Data*>();

    for (ix= owned.begin();ix != owned.end();ix++) {
        Data *child = *ix;
        if ((cname.compare(child->getName()) == 0) || (cname.compare("*")==0)) {
            v->push_back(*ix);
        }
    }

    return v;
}

const char *Data::findValueByFamily(string pfamily) {
    Data *found = findOneByFamily(pfamily);
    if (found == NULL)
        return NULL;
    else
        return found->getValue();
}

Data *Data::findOneByFamily(string cname) {
    vector<Data *>::iterator ix;
    string finder;

    for (ix= owned.begin();ix != owned.end();ix++) {
        finder.clear();
        finder.append((*ix)->getPath());

        if (cname.compare((*ix)->getFamily()) == 0) {
            return (*ix);
        }
    }

    return NULL;
}

// Private utility function used by find
void Data::findAll(vector<Data*> *v,string searchpath) {
    vector<Data *>::iterator ix;
    string finder;

    for (ix= owned.begin();ix != owned.end();ix++) {
        finder.clear();
        finder.append((*ix)->getPath());

        if (searchpath.compare(searchpath.length()-2,2,"/*") == 0) {
            finder.append("/*");
        } else {
            finder.append("/");
            finder.append((*ix)->getName());
        }

        if ((searchpath.compare(finder) == 0)) {

            v->push_back(*ix);
        } else
            (*ix)->findAll(v,searchpath);
    }

}

/*
 * serialize()
 * 
 * if container is TRUE
 * 		Construct a string to represent this object and those it contains.
 * else
 * 		Construct a string to represent this object only
 *
 * @return char * - string representing this object 
 */

char *Data::serialize(int container) {
    string s;
    s.clear();
    serialize(0,&s,container);
    return (char *) s.c_str();
}

/*
 * serialize(int indent,string *s)
 * 
 * Private function to construct the s
 *
 * @param int indent - how far to indent text
 * 
 * @param string s - append to the end of this string
 * 
 * @param int container - set to TRUE if we are dumping contained objects
 *
 * @return void
 */

void Data::serialize(int indent,string *s, int container) {
    vector<Data*>::iterator ix;
    string finder;

    s->append(indent,' ');
    s->append(path);
    s->append("/");
    s->append(name);
    s->append("\n");
    if (container) {
    indent++;
    for (ix= owned.begin();ix != owned.end();ix++) {
        (*ix)->serialize(indent,s,1);
    }
    indent--;
    }
    return;
}

// Here follow self explanatory getters and setters

string Data::getPath() {
    return path;
}

void Data::setOwner(Data *pparent) {
    owner = pparent;
    path.clear();
    if(owner != NULL) {
        path.append(owner->path);
        path.append("/");
        path.append(owner->name);
    } else {
        path.append("");
    }
}

void Data::setFamily(char *ptag) {
    family = ptag;
}

void Data::setName(char *pname) {
    name = pname;
}

void Data::setValue(char *pvalue) {
    value = pvalue;
}

void Data::addData(Data *pchild) {
    owned.push_back(pchild);
}

char *Data::getAttr(char *pname) {
    return (char *) attr[pname].c_str();
}

void Data::addAttr(char *tag,char *value) {
    attr[tag] = strdup(value);
}

const char *Data::getName() {
    return name.data();
};

const char *Data::getFamily() {
    return family.data();
};

const char *Data::getValue() {
    return value.data();
};

Data::Data() {}

Data::~Data() {
    clear();
}
