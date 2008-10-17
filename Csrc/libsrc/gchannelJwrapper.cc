//C code
#include <jni.h>
#include <stdlib.h>
#include <stdio.h>
#include <stdarg.h>
#include <string.h>
#include <strings.h>
#include "Configurer.h"
#include "gchannel.h"
#include "gchannelJwrapper.hxx"


/*----------------------------------------------------------------------------
 *  Copyright (c) 2007        Southeastern Universities Research Association,
 *                            Thomas Jefferson National Accelerator Facility
 *
 *    This software was developed under a United States Government license
 *    described in the NOTICE file included as part of this distribution.
 *
 *    Author:  heyes
 *    Created: Jun 20, 2007
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
 *      emu  - gchannel_et.c
 *
 * 	Implementation of the gchannel API using the ET system
 *
 *----------------------------------------------------------------------------*/

extern "C" JNIEXPORT jlong gchannel_open_transportJ (JNIEnv *env, jobject obj, jstring jType, jstring jName) {
    jclass myClass = env->GetObjectClass(obj);

    char *name = (char *) env->GetStringUTFChars(jName, NULL);
    char *type = (char *) env->GetStringUTFChars(jType, NULL);

    //TODO Create hash here
    gtransport trans = (gtransport) gchannel_open_transport(env, type, name);


    jfieldID gtransport_id = env->GetFieldID(myClass, "gtransport", "J" );

    env->SetLongField(obj, gtransport_id, (long) trans);
    printf( "gtransport FROM JAVA + %08x\n", (int) env->GetLongField(obj, gtransport_id ));

    env->ReleaseStringUTFChars(jName, name);
    env->ReleaseStringUTFChars(jType, type);
    return (jlong) trans;
}

extern "C" JNIEXPORT jlong gchannel_create_transportJ (JNIEnv *env, jobject obj, jstring jType, jstring jName) {


    char *name = (char *) env->GetStringUTFChars(jName, NULL);
    char *type = (char *) env->GetStringUTFChars(jType, NULL);
    printf("type is %s\n", type);
    printf("name is %s\n", name);

    gtransport trans = (gtransport) gchannel_create_transport(env, type,name);


    //TODO Create hash here
    env->ReleaseStringUTFChars(jName, name);
    env->ReleaseStringUTFChars(jType, type);
    if (trans != NULL) {
        jclass myClass = env->GetObjectClass(obj);

        jfieldID gtransport_id = env->GetFieldID(myClass, "gtransport", "J" );

        env->SetLongField(obj, gtransport_id, (long) trans);
        printf( "gtransport_id FROM JAVA + %08x\n", (int) env->GetLongField(obj, gtransport_id ));
    }
    return (jlong) trans;
}

extern "C" JNIEXPORT jlong gchannel_createJ (JNIEnv *env, jobject obj, jlong transport, jstring javaName) {


    char *name = (char *) env->GetStringUTFChars(javaName, NULL);
    jlong chan = (jlong) gchannel_create((void *) transport,name);
    //DON'T FORGET THIS LINE !!!
    env->ReleaseStringUTFChars(javaName, name);
    printf("Created channel %p\n", chan);
    return chan;
}

extern "C" JNIEXPORT jlong gchannel_openJ (JNIEnv *env, jobject obj, jlong transport, jstring javaName) {


    char *name = (char *) env->GetStringUTFChars(javaName, NULL);
    void *chan = gchannel_open((void *) transport,name);
    //DON'T FORGET THIS LINE !!!
    env->ReleaseStringUTFChars(javaName, name);

    return (jlong) chan;
}

extern "C" JNIEXPORT jlong gchannel_sendJ (JNIEnv *env, jobject obj, jlong chan, jlongArray dataJ) {
    jboolean is_copy;
    jlong *data = env->GetLongArrayElements(dataJ,&is_copy);

    gchannel_send((gchannel_hdr *)data);
    return chan;
}

extern "C" JNIEXPORT jintArray gchannel_receiveJ (JNIEnv *env, jobject obj, jlong chan) {
    long *data;
    //printf("receive on channel %d\n", chan);
    int status = gchannel_receive((void *) chan,(gchannel_hdr **) &data);
    if (status == 0 ) {
        //printf("get long array %d \n", (int32_t) data[0]);
        jintArray res = env->NewIntArray((int32_t) data[0]);
        //printf("get long array\n");
        env->SetIntArrayRegion(res, 0, (int32_t) data[0]-1, (jint*)data);

        free(data);
        return res;
    } else {
        return NULL;
    }
}

extern "C" JNIEXPORT jlong gchannel_closeJ (JNIEnv *env, jobject obj, jlong chan) {
    printf("Closing channel %08x\n", chan);
    gchannel_close((void *) chan);
    return chan;
}

extern "C" JNIEXPORT jlong gchannel_close_transportJ (JNIEnv *env, jobject obj, jlong transport) {
    gchannel_close_transport((void *) transport);
    return (jlong) transport;
}

void gchannelJwrapper::initialize(JNIEnv* env) {
    JNINativeMethod nm;
    jclass clazz;

    nm.name = "gchannel_create_transport";
    /* method descriptor assigned to signature field */
    nm.signature = "(Ljava/lang/String;Ljava/lang/String;)J";

    nm.fnPtr = (void *) gchannel_create_transportJ;

    clazz=env->FindClass("org/jlab/coda/support/transport/DataTransport");
    env->RegisterNatives(clazz,  &nm, 1);


    nm.name = "gchannel_open_transport";
    /* method descriptor assigned to signature field */
    nm.signature = "(Ljava/lang/String;Ljava/lang/String;)J";

    nm.fnPtr = (void *) gchannel_open_transportJ;

    env->RegisterNatives(clazz,  &nm, 1);

    nm.name = "gchannel_create";
    /* method descriptor assigned to signature field */
    nm.signature = "(JLjava/lang/String;)J";

    nm.fnPtr = (void *) gchannel_createJ;

    env->RegisterNatives(clazz,  &nm, 1);

    nm.name = "gchannel_open";
    /* method descriptor assigned to signature field */
    nm.signature = "(JLjava/lang/String;)J";

    nm.fnPtr = (void *) gchannel_openJ;

    env->RegisterNatives(clazz,  &nm, 1);

    nm.name = "gchannel_close";
    /* method descriptor assigned to signature field */
    nm.signature = "(J)J";

    nm.fnPtr = (void *) gchannel_closeJ;

    env->RegisterNatives(clazz,  &nm, 1);

    nm.name = "fifo_list";
    /* method descriptor assigned to signature field */
    nm.signature = "(J)J";

    nm.fnPtr = (void *) gdf_list;

    env->RegisterNatives(clazz,  &nm, 1);

    nm.name = "gchannel_close_transport";
    /* method descriptor assigned to signature field */
    nm.signature = "(J)J";

    nm.fnPtr = (void *) gchannel_close_transportJ;

    env->RegisterNatives(clazz,  &nm, 1);

    nm.name = "gchannel_send";
    /* method descriptor assigned to signature field */
    nm.signature = "(J[J)I";

    nm.fnPtr = (void *) gchannel_sendJ;

    env->RegisterNatives(clazz,  &nm, 1);

    nm.name = "gchannel_receive";
    /* method descriptor assigned to signature field */
    nm.signature = "(J)[I";

    nm.fnPtr = (void *) gchannel_receiveJ;

    env->RegisterNatives(clazz,  &nm, 1);


}

extern "C" char *gchannel_getTransportAttr(void *transport, char *name) {
    char *value = NULL;
    gtransport m = (gtransport) transport;
    JNIEnv* env = m->env;

    jclass clazz = env->FindClass("org/jlab/coda/support/transport/DataTransport");

    jmethodID getMethod = env->GetStaticMethodID(clazz, "getAttr", "(Ljava/lang/String;Ljava/lang/String;)Ljava/lang/String;");

    jstring jtname = env->NewStringUTF(m->name);
    jstring jname = env->NewStringUTF(name);

    jstring jvalue = (jstring) env->CallStaticObjectMethod(clazz, getMethod, jtname, jname);
    env->DeleteLocalRef(jname);
    env->DeleteLocalRef(jtname);

    if (env->ExceptionOccurred()) {
        env->ExceptionDescribe();
        env->ExceptionClear();
        return NULL;
    }
    if (jvalue != 0) {
        // Now convert the Java String to C++ char array
        const char* cstr = env->GetStringUTFChars(jvalue, 0);
        value = strdup(cstr);
        env->ReleaseStringUTFChars(jvalue, cstr);
        env->DeleteLocalRef(jvalue);
    }
    return value;
}
