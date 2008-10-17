#ifndef CONFIGURER_H_
#define CONFIGURER_H_

#include <jni.h>

class Configurer
{
	
public:
	Configurer();
	virtual ~Configurer();
	
	 static int getValue(JNIEnv* env,char *path, char **result);
	 static int setValue(JNIEnv* env,char *path, char *value);
	 static void setEnv(JNIEnv* env);
};

#endif /*CONFIGURER_H_*/
