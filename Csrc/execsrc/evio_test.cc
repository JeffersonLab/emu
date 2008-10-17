#include "transport_test.h"

#include "gdf.h"
#include "gsl.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <sys/types.h>
#include <unistd.h>
#include <time.h>
#include <errno.h>
#include <pthread.h>
#include <arpa/inet.h>
char *transport_test::channelNames[10];
int transport_test::channel_count;
int transport_test::size;
int transport_test::ROCID = 0;

void *read_thread(void *arg)
{
    gchannel chan = (gchannel) arg;
    printf("Thread started channel = %s\n", chan->name);

    gchannel_hdr *data;
    long counter = 0;
    double start_time = gsl_time();
    while (1)
    {
        data = NULL;
        int status = gchannel_receive(chan,&data);
        if (status != 0 && status != ETIMEDOUT)
        {
            printf("gchannel_receive returned %d, close thread\n",status);
            return NULL;
        }
        if (status != ETIMEDOUT)
        {
            counter++;
            // if (data != NULL)
            //    printf ("%s %08x %08x %08x %08x\n",chan->name,data->length,data->data[0],data->data[1],data->data[2]);
            if (counter % 10 == 0)
            {
                double now = gsl_time();
                printf ("----------------%p\n", data);
                printf ("%ld blocks in %f seconds = %2.1e blocks/s\n", counter, now - start_time, 10/(now - start_time));
                start_time = now;
                if (data != NULL)
                {
                    printf ("%08x %08x %08x %08x %08x %08x %08x\n",data->length,data->data[0],data->data[1],data->data[2],data->data[3],data->data[4],data->data[5]);
                }
            }
            gchannel_free(data);
        }
    }
    pthread_exit(NULL);
}

void *send_thread(void *arg)
{
    void *chan = arg;
    printf("Send thread started chan= %p\n",chan);
    gchannel_hdr *buf = NULL;
    int32_t *data;
    long counter = 0;
    double start_time = gsl_time();

    int len = 250000; // 1MByte buffer

    printf("Block size, Seconds per block\n");

    for (int ix=0;ix< 20000000;ix++)
    {
        int status = gchannel_allocate(chan,&buf);
        if (status == -1)
        {
            printf("status was %d buf %p\n",status,buf);
            pthread_exit(NULL);
        }
        // buffer length is "size" bytes
        int limit = transport_test::size >> 2; // limit is size of buffer in words
        int offset;
        data = buf->data;

        data[0] = htonl(0x00002000 | transport_test::ROCID | (0xff & (counter)));
        data[1] = htonl(counter++);

        offset = 2;
        int block_number = 0;
        while (1)
        {

            int block_size = 5000 + (0xff & random());
            if (offset + block_size + 1> limit)
                break;
            data[offset] = htonl(block_size);
            data[offset+1] = htonl(0x00001000 | transport_test::ROCID | (0xff & (block_number++)));
            data[offset+3] = htonl(0x1111);
            data[offset+4] = htonl(0x2222);
            data[offset+5] = htonl(0xaaaa);
            data[offset+6] = htonl(0xbbbb);
            data[offset+7] = htonl(0xcccc);
            data[offset+8] = htonl(0xdddd);

            offset += (block_size+1);
        }
        buf->length = offset + 1;
        printf("%08x %08x %08x %08x %08x %08x %08x %08x \n",buf->length,ntohl(data[0]),ntohl(data[1]),ntohl(data[2]),ntohl(data[3]),ntohl(data[4]),ntohl(data[5]),ntohl(data[6]));

        status = gchannel_send(buf);

        if (status != 0 && status != ETIMEDOUT)
        {
            printf("gchannel_send returned %d, close thread\n",status);
            pthread_exit(NULL);
        }

        if (counter % 1000 == 0)
        {
            double now = gsl_time();
            printf ("%d, %2.1e\n", buf->length, (now - start_time)/1000);
            start_time = now;
        }
    }
    pthread_exit(NULL);
}

transport_test::transport_test(char *name,char *type,int is_producer,int i)
{
    size = i;
    if (is_producer)
    {
        trans = gchannel_open_transport(NULL,type,name);
        channels[0] = (gchannel) gchannel_open(trans, channelNames[0]);
        pthread_create(&thread, NULL, send_thread, channels[0]);
    }
    else
    {
        trans = gchannel_create_transport(NULL,type, name);
        for (int ix=0;ix<channel_count;ix++)
        {
            channels[ix] = (gchannel) gchannel_create(trans,channelNames[ix]);
            printf("transport %p channel %p\n", trans, channels[ix]);
            pthread_create(&thread, NULL, read_thread, channels[ix]);
        }
    }

    pthread_join(thread, NULL);
}

transport_test::~transport_test()
{}

void transport_test::start_test()
{}

void transport_test::stop_test()
{}


int main(int argc, char **argv)
{

    int producer = false;
    char *name = NULL;
    char *input = NULL;
    char *type = NULL;
    char c;
    int size=100;

    while ((c = getopt(argc, argv, "pn:t:c:s:i:")) != EOF)
    {
        switch (c)
        {
        case 'i':
            transport_test::ROCID = atoi(optarg);
            break;
        case 's':
            size = atoi(optarg);
            break;
        case 'n':
            name =strdup(optarg);
            break;
        case 't':
            type =strdup(optarg);
            break;
        case 'p':
            producer = true;
            break;
        case 'c':
            transport_test::channelNames[transport_test::channel_count++] = strdup(optarg);
            break;
        default:
            printf("unknown option -%c/n",c);
        }
    }

    if (name == NULL)
    {
        printf("error: use -t option to specify a transport name\n");
        exit(1);
    }

    gdf_sys_init(100);

    new transport_test(name,type,producer,size);
}
