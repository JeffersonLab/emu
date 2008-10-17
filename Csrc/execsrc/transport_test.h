#ifndef TRANSPORT_TEST_H_
#define TRANSPORT_TEST_H_
#include "gchannel.h"
class transport_test {
private:
    int is_producer;
    void* trans;
    gchannel channels[10];

    pthread_t thread;

public:
	static int size;
    static char *channelNames[10];
    static int channel_count;
    static int ROCID;
    transport_test(char *name,char *type,int is_producer,int size);
    void start_test();
    void stop_test();
    virtual ~transport_test();
};

#endif /*TRANSPORT_TEST_H_*/
