#include "Configurer.h"
#include <stdlib.h>




Configurer::Configurer() {}

Configurer::~Configurer() {}

int Configurer::getValue(JNIEnv* env, char *path,char **result) {
	char *svalue;
	printf("Env %p\n", env);

    jclass theClass = env->FindClass("org/jlab/coda/support/config/Configurer");
    printf("Class = %p\n", theClass);
    jmethodID theMethod = env->GetStaticMethodID(theClass, "getValue", "(Ljava/lang/String;)Ljava/lang/String;");
    printf("Method %p\n", theMethod);
    jstring jpath = env->NewStringUTF(path);
    printf("jpath %p\n", jpath);
    jstring jres = (jstring)env->CallStaticObjectMethod(theClass, theMethod, jpath);
    printf("res %p\n", jres);
    env->ReleaseStringUTFChars(jpath, path);
    if (env->ExceptionOccurred()) {
        env->ExceptionDescribe();
        env->ExceptionClear();
        return -1;
    }
    svalue = (char *)env->GetStringUTFChars(jres, NULL);
    env->ReleaseStringUTFChars(jres, svalue);
    *result = svalue;
    return 0;
}

int Configurer::setValue(JNIEnv* env, char *path, char *value) {
    jclass clazz = env->FindClass("org/jlab/coda/support/config/Configurer");
    printf("Class = %p\n", clazz);
    jmethodID method = env->GetStaticMethodID(clazz, "setValue", "(Ljava/lang/String;Ljava/lang/String;)V");
    printf("Method %p\n", method);
    jstring jpath = env->NewStringUTF(path);
    printf("jpath %p\n", jpath);
    jstring jvalue = env->NewStringUTF(value);
    
    jobjectArray args = (jobjectArray) env->NewObjectArray(2, env->FindClass("java/lang/String"), NULL);
    
    env->SetObjectArrayElement(args, 0, jpath);
    env->SetObjectArrayElement(args, 0, jvalue);
    
 	env->CallStaticVoidMethod(clazz, method, args);
    if (env->ExceptionOccurred()) {
        env->ExceptionDescribe();
        env->ExceptionClear();
        return -1;
    }

    return 0;
}
