/*----------------------------------------------------------------------------
 *  Copyright (c) 2007        Southeastern Universities Research Association,
 *                            Thomas Jefferson National Accelerator Facility
 *
 *    This software was developed under a United States Government license
 *    described in the NOTICE file included as part of this distribution.
 *
 *    Author:  heyes
 *    Created: Jun 14, 2007
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
 *      emu  - emu_xml_parser.c Parse XML configuration file.
 *
 *----------------------------------------------------------------------------*/

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "parsifal/parsifal.h"
#include "parsifal/dtdvalid.h"

#include "gph.h"
#include "gll.h"
#include "gparse.h"

typedef struct generic_parser {
    gll_st parser_stack;
    element_struct *current_element;
}
generic_parser_struct;

typedef struct gparse_handler {
    char *tag;
    char *context;
    void (*handler)(struct element *);
}
gparse_handler_struct;

static gll_li gparse_handlers;

static void ErrorHandler(LPXMLPARSER parser) {
    if (parser->ErrorCode == ERR_XMLP_VALIDATION) {
        LPXMLDTDVALIDATOR vp = (LPXMLDTDVALIDATOR)parser->UserData;
        printf("Validation Error: %s\nErrorLine: %d ErrorColumn: %d\n",
               vp->ErrorString, vp->ErrorLine, vp->ErrorColumn);
    } else {
        printf("Parsing Error: %s\nErrorLine: %d ErrorColumn: %d\n",
               parser->ErrorString, parser->ErrorLine, parser->ErrorColumn);
    }
}

void test_handler(struct element *el) {
    printf("HANDLER SEES %s\n",el->tag);
}

static void param_handler(struct element *el) {
    int i;
    char *name;
    char key = GPH_NO_KEY;
    char *help = "";
    char *value;

    if (el->attr != NULL) {
        for (i=0;i<el->n_attr;i++) {
            if (strcmp((char *) el->attr[i].tag, "name") == 0) {
                name = (char *) el->attr[i].value;
            } else if (strcmp((char *) el->attr[i].tag, "value") == 0) {
                value = (char *) el->attr[i].value;
            } else if (strcmp((char *) el->attr[i].tag, "help") == 0) {
                help = (char *) el->attr[i].value;
            } else if (strcmp((char *) el->attr[i].tag, "key") == 0) {
                key = el->attr[i].value[0];

            }
        }
    }

    gph_add_param(name,key,help,value);

}
void gparse_register(char *tag_name, char *context, void (*handler_rtn)(struct element *)) {
    if (gparse_handlers == NULL)
        gparse_handlers = gll_create_li("XML parse handlers");

    gparse_handler_struct *handler = malloc(sizeof(gparse_handler_struct));

    handler->tag = strdup(tag_name);
    handler->context = strdup(context);
    handler->handler = handler_rtn;

    gll_add_el(gparse_handlers,handler);
}

static int cstream(BYTE *buf, int cBytes, int *cBytesActual, void *inputData) {
    *cBytesActual = fread(buf, 1, cBytes, (FILE*)inputData);
    return (*cBytesActual < cBytes);
}

static int StartElement(void *UserData, const XMLCH *uri, const XMLCH *localName, const XMLCH *qName, LPXMLVECTOR atts) {

    LPXMLDTDVALIDATOR v = (LPXMLDTDVALIDATOR)UserData;
    generic_parser_struct *param_parser = (generic_parser_struct *) v->UserData;
    gll_st parser_stack = param_parser->parser_stack;

    int i;

    element_struct *element = malloc(sizeof(element_struct));

    bzero ((void *) element, sizeof(element_struct));

    if (param_parser->current_element) {
        element->context = malloc(strlen(param_parser->current_element->tag)+strlen(param_parser->current_element->context)+2);
        sprintf (element->context,"%s/%s",param_parser->current_element->context,param_parser->current_element->tag);
        gll_push(parser_stack,param_parser->current_element);
    } else
        element->context = strdup("");

    param_parser->current_element = element;

    element->tag = strdup((char *) qName);
    element->attr = NULL;
    element->value = NULL;
    element->n_attr = atts->length;
    //printf("tag %s in context %s\n",element->tag,element->context);
    if (atts->length) {
        int i,j;
        LPXMLRUNTIMEATT att;

        element->attr = malloc(sizeof(attr_struct)*atts->length);

        for (i=0; i<atts->length; i++) {
            att = (LPXMLRUNTIMEATT) XMLVector_Get(atts, i);

            element->attr[i].tag = strdup((char *) att->qname);
            element->attr[i].value = strdup((char *) att->value);
        }
    }

    return 0;
}

static int EndElement(void *UserData, const XMLCH *uri, const XMLCH *localName, const XMLCH *qName) {
    LPXMLDTDVALIDATOR v = (LPXMLDTDVALIDATOR)UserData;
    generic_parser_struct *param_parser = (generic_parser_struct *) v->UserData;
    gll_st parser_stack = param_parser->parser_stack;

    element_struct *element = param_parser->current_element;
    int i;

    // do something with the element

    if (gparse_handlers != NULL) {

        gll_el el = gll_get_first(gparse_handlers);

        while (el != NULL) {
            gparse_handler_struct *handler = gll_get_data(el);
            if  ((strcmp(handler->context, element->context) == 0) &&
                    (strcmp(handler->tag, element->tag) == 0)) {
                (*(handler->handler))(element);
                // keep looking for other handlers
                // for same tag.
            }
            el = gll_get_next(el);
        }

    }

    if (element->value != NULL) {
        char *path, *value, *oldvalue;
        path = malloc(strlen(element->context) + strlen(element->tag) + 2);
        sprintf(path,"%s/%s",element->context,element->tag);
        value = element->value;
        oldvalue = (char *) gph_get_param(path);

        if (oldvalue !=NULL) {
            value = malloc(strlen(oldvalue) + strlen(element->value) + 2);
            sprintf(value,"%s,%s",oldvalue,element->value);

        }
        printf("%s = %s\n",path,value);


        gph_add_param(path,'\0',"",value);
    }

    //printf("<%s ", element->tag);

    if (element->attr != NULL) {
        for (i=0;i<element->n_attr;i++) {
            //printf(" %s=\"%s\"", element->attr[i].tag, element->attr[i].value);
            free(element->attr[i].tag);
            free(element->attr[i].value);
        }
        free(element->attr);
    }
    /*    if (element->value)
            printf(">\n\t%s\n</%s>\n",element->value, element->tag);
        else
            printf(" />\n");*/

    free(element->tag);
    free(element->context);
    if (element->value)
        free(element->value);
    free(element);

    param_parser->current_element = gll_pop(parser_stack);

    return 0;
}

static int Characters(void *UserData, const XMLCH *Chars, int cbChars) {
    LPXMLDTDVALIDATOR v = (LPXMLDTDVALIDATOR)UserData;
    generic_parser_struct *param_parser = (generic_parser_struct *) v->UserData;

    char *c = strdup((char *) Chars);

    c[cbChars] = '\0';

    param_parser->current_element->value = strdup(c);

    free(c);
    return XML_OK;
}

int gparse_file(char *filename) {
    LPXMLPARSER parser;
    LPXMLDTDVALIDATOR vp;
    static int first_time = 1;

    generic_parser_struct param_parser;

    if (filename == NULL)
        return -1;

    printf("Parse parameter file %s\n",filename);
    FILE *f = fopen(filename,"r");
    if (f == NULL) {
        perror("problem opening file :\n");
        return -1;
    }

    if (first_time) {
        gparse_register("param","/component/parameters",param_handler);
        first_time = 0;
    }

    param_parser.parser_stack = gll_create_st();
    param_parser.current_element = NULL;

    if (!XMLParser_Create(&parser)) {
        printf("Error creating parser!\n");
        return 1;
    }

    vp = XMLParser_CreateDTDValidator();
    if (!vp) {
        puts("Error creating DTDValidator in main()");
        return 1;
    }
    parser->errorHandler = ErrorHandler;
    parser->startElementHandler = StartElement;
    parser->endElementHandler = EndElement;
    parser->charactersHandler = Characters;

    vp->UserData = &param_parser;

    if (!XMLParser_ParseValidateDTD(vp,parser, cstream, f, 0)) {
        printf("Error: %s\nLine: %d Col: %d\n",
               parser->ErrorString, parser->ErrorLine, parser->ErrorColumn);
        return -1;
    }

    XMLParser_FreeDTDValidator(vp);
    XMLParser_Free(parser);
    fclose(f);

    return 0;
}

/*int main(int argc, char* argv[]) {
    return	parse_parameters("parameters.xml");
}*/

