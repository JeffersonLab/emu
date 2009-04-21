
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
 *      emu  - gchannel_so.c
 *
 * 	Implementation of the gchannel API using TCP sockets
 *
 *----------------------------------------------------------------------------*/

#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <fcntl.h>
#include <netdb.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <unistd.h>
#include <errno.h>
#include <sys/uio.h>
#include <sys/time.h>
#include <sys/select.h>
#include <sys/utsname.h>
#include <netinet/tcp.h>
#ifdef __APPLE__
#include <ifaddrs.h>
#endif

#include "gchannel.h"
#include "gph.h"

#include "jni.h"

typedef  struct so_transport_st *so_transport;

typedef struct so_transport_st
{
    int capacity;
    int length;
    int port;
    char *name;
    char *host;
    char *listen_host;
    int owner;
    int32_t listen_sock;
    int32_t multicast_sock;
    struct sockaddr_in listenAddr;
    pthread_t accept_helper;
    pthread_t multicast_helper;
    gll_li channels;
}
so_transport_ty;

typedef struct so_channel_st *so_channel;

typedef struct so_channel_st
{
    gchannel c;
    char *name;
    int run;
    int active;
    int owner;
    int32_t dataSock;
    pthread_t helper;
    so_transport transp;
}
so_channel_ty;

so_channel so_find_channel(so_transport trans, char *name)
{
    if ((trans == NULL) || (name == NULL))
        return NULL;
    so_channel chan = NULL;
    gll_li channels = trans->channels;

    for (gll_el el = gll_get_first(channels);el!=NULL;el = gll_get_next(el))
    {
        so_channel tmp = (so_channel) gll_get_data(el);

        if (strcmp(name,tmp->name) == 0 )
        {
            chan = tmp;
            break;
        }
    }

    return chan;
}

extern "C" int gchannel_net_send(int socket, char *data, int length)
{
    int rest = length;
    char *p = data;
    int actual = 0;
    while (rest>0)
    {
        actual = send(socket, p, rest, 0);
        if (actual < 0)
            return actual;
        p += actual;
        rest -= actual;
    }
    return length;
}

extern "C" void *gchannel_read_helper_so(void *arg);

extern "C" void *gchannel_accept_helper_so(void *arg)
{
    gtransport m = (gtransport) arg;
    so_transport transp = (so_transport) m->hidden;
    int len = 0;
    transp->listen_sock = socket(PF_INET, SOCK_STREAM, 0);
    int window_size = 40000;
    int flag = 1;

    if(-1 == transp->listen_sock)
    {
        printf("can not create socket");
        transp->listen_sock = -1;
        goto gchannel_accept_helper_so_end;
    }

    setsockopt(transp->listen_sock, SOL_SOCKET, SO_SNDBUF,
               (char *) &window_size, sizeof(window_size));


    setsockopt(transp->listen_sock, IPPROTO_TCP, TCP_NODELAY,
               (char *) &flag, sizeof(flag));

    char hostname[1000];
    gethostname(hostname,1000);
    printf("hostname is %s\n", hostname);

    struct hostent *hostentry = gethostbyname(hostname);

    bzero(&transp->listenAddr, sizeof(transp->listenAddr));

    transp->listenAddr.sin_family = AF_INET;
    transp->listenAddr.sin_port = 0;
    bcopy(hostentry->h_addr_list[0], (char *) &transp->listenAddr.sin_addr, sizeof(transp->listenAddr.sin_addr));

    printf("\tassociated address : %s\n", inet_ntoa(transp->listenAddr.sin_addr));
    transp->listen_host = strdup(inet_ntoa(transp->listenAddr.sin_addr));
    if(-1 == bind(transp->listen_sock,(struct sockaddr*) &transp->listenAddr, sizeof(transp->listenAddr)))
    {
        printf("error bind failed");
        close(transp->listen_sock);
        transp->listen_sock = -1;
        goto gchannel_accept_helper_so_end;
    }
    len = sizeof(transp->listenAddr);
    getsockname(transp->listen_sock,(struct sockaddr*) &transp->listenAddr, (socklen_t*) &len);

    transp->port = ntohs(transp->listenAddr.sin_port);

    if (-1 == listen(transp->listen_sock, 100))
    {
        printf("error listen failed");
        close(transp->listen_sock);
        transp->listen_sock = -1;
        goto gchannel_accept_helper_so_end;

    }

    printf("listening for connections on %s : %d\n", inet_ntoa(transp->listenAddr.sin_addr), transp->port);

    while(transp->listen_sock > 0)
    {
        pthread_testcancel();

        int32_t i32ConnectFD = accept(transp->listen_sock, NULL, NULL);
        char tmp[1000];
        bzero(tmp,1000);
        if(0 > i32ConnectFD)
        {
            printf("error accept failed");

            close(transp->listen_sock);
            transp->listen_sock = -1;

            goto gchannel_accept_helper_so_end;
        }
        char l;
        read(i32ConnectFD,&l,1);
        printf("got a connection %d\n",i32ConnectFD);
        printf("length of name is %d\n",l);
        int actual = read(i32ConnectFD,tmp,l);
        printf("read %d bytes %s\n", actual,tmp);

        printf("[%s]\n",tmp);
        so_channel chan = so_find_channel(transp,tmp);
        if (chan != NULL)
        {
            printf("associating socket %d with channel %s\n", i32ConnectFD, chan->name);
            chan->dataSock = i32ConnectFD;
            sprintf(tmp,"%s channel helper",chan->name);
            pthread_create(&chan->helper,NULL, gchannel_read_helper_so, (void *) chan);
        }
        else
        {
            printf("No data channel named %s was found\n", tmp);
        }
    }
gchannel_accept_helper_so_end:
    pthread_exit(NULL);
    return NULL;

}
extern "C" void *gchannel_multicast_helper_so(void *arg)
{
    gtransport m = (gtransport) arg;
    so_transport transp = (so_transport) m->hidden;

    printf("\n\ngchannel_multicast_helper_so: listening %d\n",transp->multicast_sock > 0);
    for ( ; transp->multicast_sock > 0 ; )
    {

        pthread_testcancel();

        struct sockaddr_in cliaddr;
        int nbytes, slen = sizeof(cliaddr);
        bzero(&cliaddr,sizeof(cliaddr));
#define MCAST_BUFFER_LENGTH 1000

        char buffer[MCAST_BUFFER_LENGTH];
        bzero(buffer,MCAST_BUFFER_LENGTH);
        printf("call recvfrom\n");
        /* read incoming data */
        nbytes = recvfrom(transp->multicast_sock, (void *) buffer, MCAST_BUFFER_LENGTH,
                          0, (sockaddr*) &cliaddr, (socklen_t*)&slen);

        if (nbytes <=0)
        {
            printf("gchannel_multicast_helper_so recvfrom %d\n", nbytes);
            goto gchannel_multicast_helper_so_end;
        }
        printf("got %s\n", buffer);

        if (strcmp(buffer,transp->name) == 0)
        {
            sprintf(buffer,"%s %s %d",transp->name,transp->listen_host,transp->port);

            printf("sending \"%s\" back\n", buffer);

            sendto(transp->multicast_sock, (void*) buffer, strlen(buffer), 0,(sockaddr*) &cliaddr, slen);
        }
    }

gchannel_multicast_helper_so_end:
    pthread_exit(NULL);
    return NULL;

}

extern "C" int gchannel_create_transport_so(gtransport m)
{
    char tmp[1000];
    so_transport transp = (so_transport) malloc(sizeof(so_transport_ty));

    bzero (transp,sizeof(so_transport_ty));
    m->hidden = transp;

    transp->name = strdup(m->name);
    transp->capacity = 2;
    transp->length = 32000000;
    transp->owner = true;
    transp->port=7030;
    m->channels = transp->channels = gll_create_li(m->name);

    if (m->env != NULL)
    {
        char *value = gchannel_getTransportAttr(m, "pool");

        if (value == NULL )
            return gchannel_exception(-1,(void *) m,"et_start pool attribite missing");

        transp->capacity = atoi(value);

        value = gchannel_getTransportAttr(m, "size");

        if (value == NULL )
            return gchannel_exception(-1,(void *) m,"et_start size attribite missing");

        transp->length = atoi(value);

        value = gchannel_getTransportAttr(m, "port");

        if (value != NULL)
            transp->port = atoi(value);

    }
    sprintf(tmp,"%s accept helper",transp->name);
    pthread_create(&transp->accept_helper, NULL, gchannel_accept_helper_so, (void *) m);
    sprintf(tmp,"%s multicast helper",transp->name);

    struct ip_mreq     mreq;
    int on, err;

    struct sockaddr_in castaddr;
    struct sockaddr_in serveraddr;

    bzero(&castaddr, sizeof(castaddr));
    bzero(&serveraddr, sizeof(serveraddr));
    bzero(&mreq, sizeof(mreq));

    castaddr.sin_family = AF_INET;
    castaddr.sin_port = htons(transp->port);
    inet_aton("239.200.0.0", &castaddr.sin_addr);

    transp->multicast_sock = socket(AF_INET, SOCK_DGRAM, 0);
    if (transp->multicast_sock < 0)
    {
        fprintf(stderr, "et_udpreceive: socket error\n");
        return -1;
    }

    on = 1;
    err = setsockopt(transp->multicast_sock, SOL_SOCKET, SO_REUSEADDR, (void *) &on, sizeof(on));
    if (err < 0)
    {
        fprintf(stderr, "et_udpreceive: setsockopt error\n");
        close(transp->multicast_sock);
        transp->multicast_sock = -1;
        return -1;
    }

    /* bind port */
    serveraddr.sin_family=AF_INET;
    serveraddr.sin_addr.s_addr=htonl(INADDR_ANY);
    serveraddr.sin_port=htons(transp->port);

    if(bind(transp->multicast_sock,(struct sockaddr *) &serveraddr, sizeof(serveraddr))<0)
    {
        char errnostr[255];
        sprintf(errnostr,"err=%d ",errno);
        perror(errnostr);
        fprintf(stderr, "et_udpreceive: bind error 239.200.0.0:7030\n");
        close(transp->multicast_sock);
        return -1;
    }
    bcopy(&castaddr.sin_addr,&mreq.imr_multiaddr.s_addr,sizeof(struct in_addr));

    /* accept multicast over any interface */
    mreq.imr_interface.s_addr = htonl(INADDR_ANY);
    err = setsockopt(transp->multicast_sock, IPPROTO_IP, IP_ADD_MEMBERSHIP, (void *) &mreq, sizeof(mreq));
    if (err < 0)
    {
        fprintf(stderr, "et_udpreceive: setsockopt IP_ADD_MEMBERSHIP error\n");
        close(transp->multicast_sock);
        return -1;
    }

    pthread_create(&transp->multicast_helper,NULL, gchannel_multicast_helper_so, (void *) m);
    return 0;
}

extern "C" void *gchannel_read_helper_so(void *arg)
{
    so_channel chan = (so_channel) arg;
    gchannel c = chan->c;

    struct timespec wait;

    wait.tv_nsec = 0;
    wait.tv_sec = 1;

    chan->active = TRUE;
    printf("gchannel_read_helper_so : thread starts\n");

    chan->run = TRUE;

    while((c->fifo_aux > 0) && chan->run)
    {

        pthread_testcancel();

        int status;
        gchannel_hdr *in_data;

        c->record_count++;
        c->word_count += 1234;

        do
        {
            pthread_testcancel();
            // gdf_get will return a status of ETIMEDOUT after 0.25 seconds.
            status = gdf_get(c->fifo_aux, (long int **) &in_data);

            if (chan->run == FALSE)
            {

                goto gchannel_read_helper_so_end;
            }
        }
        while (status == ETIMEDOUT);

        //printf ("gchannel_read_helper_so : got record number %lld\n",c->record_count);

        int bytes = recv(chan->dataSock,&in_data->length,sizeof(in_data->length),MSG_WAITALL);

        if (bytes != sizeof(in_data->length))
        {
            printf("length not 4 bytes long! it's %d\n",bytes);
            goto gchannel_read_helper_so_end;
        }

        printf("gchannel_read_helper_so : length = %08x", in_data->length);
        // Always transmit lengths in network byte order so translate back.
        in_data->length = ntohl(in_data->length);
        printf("gchannel_read_helper_so : length = ntohl %08x", ntohl(in_data->length));
        bytes = recv(chan->dataSock,&in_data->data,in_data->length*sizeof(int32_t),MSG_WAITALL);

        // printf("gchannel_read_helper_so %s %08x %08x %08x \n",chan->name,in_data->data[0],in_data->data[1],in_data->data[2]);

        // printf("gchannel_read_helper_so length is %ld bytes %d\n",in_data->length * sizeof(int32_t), bytes);

        // printf("gchannel_read_helper_so Put on %08x\n",c->fifo);

        do
        {
            pthread_testcancel();
            // gdf_get will return a status of ETIMEDOUT after 0.25 seconds.
            status = gdf_put(c->fifo, (long int *) in_data);

            if (chan->run == FALSE)
            {

                goto gchannel_read_helper_so_end;
            }
        }
        while (status == ETIMEDOUT);
        char ack = 0xaa;

        gchannel_net_send(chan->dataSock, (char *) &ack, sizeof(ack));

    }
gchannel_read_helper_so_end:
    chan->run = FALSE;
    chan->active = FALSE;
    pthread_exit(NULL);
    return NULL;
}

extern "C" int gchannel_create_so(gchannel c)
{
    char tmp[100];

    so_transport transp = (so_transport) c->transport->hidden;
    so_channel chan = (so_channel) malloc(sizeof(so_channel_ty));
    bzero(chan, sizeof(so_channel_ty));

    c->hidden = chan;
    chan->c = c;

    chan->run = TRUE;
    chan->owner = TRUE;
    chan->transp = transp;
    sprintf(tmp,"%s channel fifo",c->name);
    c->fifo = gdf_new_sized(tmp, transp->capacity);
    sprintf(tmp,"%s channel auxoutput",c->name);
    c->fifo_aux = gdf_new_sized(tmp, transp->capacity);
    for (int ix=0; ix< transp->capacity; ix++)
    {
        gchannel_hdr *buf = (gchannel_hdr *) malloc(transp->length+sizeof(gchannel_hdr));
        //printf("buffer %d is %p\n",ix,buf);
        buf->channel = c;
        gdf_put(c->fifo_aux,(long int *) buf);
    }
    chan->name = c->name;

    gll_add_el(transp->channels, (void *) chan);

    return 0;
}

extern "C" int gchannel_open_transport_so(gtransport m)
{

    struct timespec time_to_wait;
    time_to_wait.tv_nsec = 0;
    time_to_wait.tv_sec = 4;

    so_transport transp = (so_transport) malloc(sizeof(so_transport_ty));

    bzero (transp,sizeof(so_transport_ty));
    m->hidden = transp;

    transp->name = strdup(m->name);
    transp->owner = false;
    transp->port = 7030;
    transp->host = strdup("albanac");
    transp->capacity = 2;
    transp->length = 32000000;

    m->channels = transp->channels = gll_create_li(m->name);

    if (m->env != NULL)
    {
        char *value = gchannel_getTransportAttr(m, "pool");

        if (value != NULL)
            transp->capacity = atoi(value);

        value = gchannel_getTransportAttr(m, "size");

        if (value != NULL)
            transp->length = atoi(value);

        value = gchannel_getTransportAttr(m, "port");

        if (value != NULL)
            transp->port = atoi(value);

        value = gchannel_getTransportAttr(m, "host");

        if (value != NULL)
        {
            free(transp->host);
            transp->host = strdup(value);
        }
    }

    struct sockaddr_in castaddr,cliAddr;
    bzero(&castaddr, sizeof(castaddr));
    bzero(&cliAddr, sizeof(cliAddr));
    castaddr.sin_family = AF_INET;
    castaddr.sin_port = htons(transp->port);
    inet_aton("239.200.0.0", &castaddr.sin_addr);

    /* create socket */
    transp->multicast_sock = socket(AF_INET, SOCK_DGRAM, 0);
    if (transp->multicast_sock < 0)
    {
        fprintf(stderr, "et_findserver: socket error\n");

        return -1;
    }

    unsigned char ttl = 1; // TODO change this!

    int err = setsockopt(transp->multicast_sock, IPPROTO_IP, IP_MULTICAST_TTL, (void *) &ttl, sizeof(ttl));
    if (err < 0)
    {
        fprintf(stderr, "et_findserver: setsockopt IP_MULTICAST_TTL error\n");
        close(transp->multicast_sock);

        return -1;
    }
    /* bind any port number */
    cliAddr.sin_family = AF_INET;
    cliAddr.sin_addr.s_addr = htonl(INADDR_ANY);
    cliAddr.sin_port = htons(0);

    if(bind(transp->multicast_sock,(struct sockaddr *) &cliAddr,sizeof(cliAddr))<0)
    {
        perror("bind");
        exit(1);
    }
    char buffer[1000];

    int flags = fcntl(transp->multicast_sock, F_GETFL);
    flags |= O_NONBLOCK;
    fcntl(transp->multicast_sock, F_SETFL, flags);



    /* get back a packet from a server */

    int len;


    int loopcount, msglen;
    struct timespec delaytime;
    delaytime.tv_sec  = 1;
    delaytime.tv_nsec = 0;

    for (loopcount = 0;loopcount<10;loopcount++)
    {
        sendto(transp->multicast_sock, (void *) transp->name, strlen(transp->name), 0, (sockaddr *) &castaddr, sizeof(castaddr));
        printf("wait for response\n");
        nanosleep(&delaytime, NULL);
        struct sockaddr_in response_addr;
        len = sizeof(response_addr);
        bzero((char*) buffer,sizeof(buffer));
        msglen = recvfrom(transp->multicast_sock, (void *) buffer, sizeof(buffer), 0, (sockaddr *) &response_addr, (socklen_t*)&len);

        printf("got response %d characters long %s\n",msglen,buffer);
        if (msglen >0)
        {
            char host[1000],name[1000];
            int port;
            sscanf(buffer,"%s %s %d",name,host,&port);

            printf("got %s %s %d\n", name, host, port);
            printf("compare with %s\n", transp->name);
            if (strcmp(name,transp->name) == 0)
            {
                printf ("Server response matches transport name %s\n", transp->name);
                transp->listen_host = strdup(host);
                transp->port = port;
                return 0;
            }
            else
            {
                continue;
            }

        }
    }

    close(transp->multicast_sock);
    printf("timeout waiting for server response to multicast\n");
    return -1;

}

extern "C" void *gchannel_send_helper_so(void *arg)
{
    gchannel c = (gchannel) arg;

    so_channel chan = (so_channel) c->hidden;

    gchannel_hdr *out_data = NULL;

    int status;

    struct timespec wait;

    wait.tv_nsec = 0;
    wait.tv_sec = 1;

    chan->active = TRUE;

    while (chan->run )
    {
        pthread_testcancel();
        gchannel_hdr *out_data = NULL;

        int status;

        //printf("gchannel_send_helper_so thread wait for data\n");
        do
        {
            // gdf_get will return a status of ETIMEDOUT after 0.25 seconds.
            status = gdf_get(c->fifo, (long int **)&out_data);


            if (!chan->run)
            {
                printf("chan-> run is false\n");
                goto gchannel_send_helper_so_end;
            }
        }
        while (status == ETIMEDOUT);

        //printf("gchannel_send_helper_so: %s %d\n",c->name,out_data->length*sizeof(int32_t));

        int32_t net_length = htonl(out_data->length);

        if ( gchannel_net_send(chan->dataSock, (char *) &net_length, sizeof(net_length)) < 0)
        {
            printf("gchannel_net_send send length failed\n");
            goto  gchannel_send_helper_so_end;
        }

        if ( gchannel_net_send(chan->dataSock, (char *) &out_data->data, out_data->length*sizeof(int32_t)) < 0)
        {
            printf("gchannel_net_send send length failed\n");
            goto  gchannel_send_helper_so_end;
        }

        char ack = 0;

        int recvd = read(chan->dataSock,&ack,sizeof(ack));

        if ((recvd !=sizeof(ack)) || (ack != (char) 0xaa))
        {
            printf("recvd is %d ack is %02x\n",recvd, ack);
            goto gchannel_send_helper_so_end;
        }

        //printf("gchannel_send_helper_so: recvd ack %d\n", recvd);
        do
        {
            pthread_testcancel();
            // gdf_get will return a status of ETIMEDOUT after 0.25 seconds.
            status = gdf_put(c->fifo_aux, (long int *) out_data);

            if (!chan->run)
            {
                printf("chan-> run is false after gdf_put\n");
                goto gchannel_send_helper_so_end;
            }
        }
        while (status == ETIMEDOUT);
        out_data = NULL;
    }

gchannel_send_helper_so_end:
    if (out_data != NULL)
        do
        {
            // gdf_get will return a status of ETIMEDOUT after 0.25 seconds.
            status = gdf_put(c->fifo_aux, (long int *) out_data);
            pthread_testcancel();
            if (chan->run == FALSE)
            {
                break;
            }
        }
        while (status == ETIMEDOUT);
    printf("gchannel_send_helper_so_end %d\n",chan->run);
    chan->run = FALSE;
    chan->active = FALSE;
    pthread_exit(NULL);
    return (NULL);
}

extern "C" int gchannel_open_so(gchannel c)
{
    so_transport transp = (so_transport) c->transport->hidden;

    so_channel chan = (so_channel) malloc(sizeof(so_channel_ty));
    bzero(chan, sizeof(so_channel_ty));
    c->hidden = chan;
    chan->c = c;
    chan->run = TRUE;
    chan->owner = FALSE;
    chan->transp= transp;
    char tmp[1000];

    chan->dataSock = socket(PF_INET, SOCK_STREAM, 0);

    if(-1 == chan->dataSock)
    {
        printf("cannot create socket")
        ;
        exit(-1);
    }

    bzero(&transp->listenAddr, sizeof(transp->listenAddr));

    transp->listenAddr.sin_family = AF_INET;
    transp->listenAddr.sin_port = htons(transp->port);
    transp->listenAddr.sin_addr.s_addr = inet_addr(transp->listen_host);

    printf("call connect on %s port %d\n", inet_ntoa(transp->listenAddr.sin_addr), transp->port);
    int window_size = 40000;
    setsockopt(chan->dataSock, SOL_SOCKET, SO_SNDBUF,
               (char *) &window_size, sizeof(window_size));

    int flag = 1;
    setsockopt(chan->dataSock, IPPROTO_TCP, TCP_NODELAY,
               (char *) &flag, sizeof(flag));


    if(-1 == connect(chan->dataSock,(struct sockaddr*) &transp->listenAddr, sizeof(transp->listenAddr))
      )
    {
        printf("connect failed");
        exit(-1);
    }

    printf("sending %s which is %d bytes long\n",c->name,strlen(c->name));
    char l = strlen(c->name);
    gchannel_net_send(chan->dataSock,&l,1);
    gchannel_net_send(chan->dataSock,c->name,strlen(c->name));

    sprintf(tmp,"%s channel fifo",c->name);

    c->fifo = gdf_new_sized(tmp, transp->capacity);
    sprintf(tmp,"%s channel aux fifo",c->name);
    c->fifo_aux = gdf_new_sized(tmp, transp->capacity);
    for (int ix=0; ix< transp->capacity; ix++)
    {
        gchannel_hdr *buf = (gchannel_hdr *) malloc(transp->length+sizeof(gchannel_hdr));
        //printf("buffer %d is %p\n",ix,buf);
        buf->channel = c;
        gdf_put(c->fifo_aux,(long int *) buf);
    }
    // open is used by writers so set up a thread to get records from the FIFO and put them to the ET.
    sprintf(tmp,"%s channel helper",c->name);
    pthread_create(&chan->helper, NULL, gchannel_send_helper_so, (void *) c);

    gll_add_el(transp->channels, (void *) chan);
    return 0;
}

extern "C" int gchannel_send_so(gchannel_hdr *buffer)
{
    return gdf_put(buffer->channel->fifo,(long *) buffer);
}

extern "C" int gchannel_receive_so(gchannel channel, gchannel_hdr *buffer)
{
    //printf("gchannel_receive_so : waiting on fifo for data\n");
    int status = gdf_get(channel->fifo, (long **)buffer);
    //printf("gchannel_receive_so : gdf_get returns %d %p \n",status,*buffer);
    return status;
}

extern "C" int gchannel_close_transport_so(gtransport m)
{
    printf("GTRANSPORT_CLOSE_SO\n");
    so_transport transp = (so_transport) m->hidden;

    if (transp->owner)
    {
        printf("close multicast_sock\n");
        if (transp->multicast_sock != -1)
        {
            shutdown(transp->multicast_sock, SHUT_RDWR);
            close(transp->multicast_sock);
        }
        printf("close listen_sock\n");
        if (transp->listen_sock != -1)
        {
            shutdown(transp->listen_sock, SHUT_RDWR);
            close(transp->listen_sock);
        }
        printf("delete channels list\n");
        gll_delete_li(transp->channels);
        printf("WAIT transp->accept_helper %p\n",transp->accept_helper);
        //pthread_cancel(transp->accept_helper);
        pthread_join(transp->accept_helper,NULL);
        printf("WAIT transp->multicast_helper %p\n",transp->multicast_helper);
        //pthread_cancel(transp->multicast_helper);
        pthread_join(transp->multicast_helper,NULL);
    }
    else
    {
        gll_delete_li(transp->channels);
        free(transp->host);
        free(transp->listen_host);
    }
    free(transp->name);
    free(transp);
    printf("GTRANSPORT_CLOSE_SO Done\n");

    return 0;
}

extern "C" int gchannel_allocate_so(gchannel c, gchannel_hdr **data)
{
    int status;
    so_channel chan = (so_channel) c->hidden;
    //printf("gchannel_allocate_so wait for data\n");
    do
    {

        // gdf_get will return a status of ETIMEDOUT after 0.25 seconds.
        status = gdf_get(c->fifo_aux,(long **) data);
        //printf("got %p with status %d\n", data, status);
        if (chan->run == FALSE)
        {
            printf("chan->run is FALSE\n");
            if (status == 0)
                gdf_put(c->fifo_aux, (long *) *data);
            return -1;
        }
    }
    while (status == ETIMEDOUT);

    return 0;
}

extern "C" int gchannel_free_so(gchannel_hdr *buf)
{
    buf->length = 0;
    gdf_put(buf->channel->fifo_aux, (long *) buf);
    return 0;
}

extern "C" int gchannel_close_so(gchannel c)
{
    printf("GCHANNEL_CLOSE_SO\n");
    so_transport transp = (so_transport) c->transport->hidden;
    so_channel chan = (so_channel) c->hidden;
    chan->run = FALSE;

    gll_el el = gll_find_el(transp->channels, (void *) chan);

    gll_remove_el(el);

    shutdown(chan->dataSock, SHUT_RDWR);
    close(chan->dataSock);
    printf("chan->helper is %08x\n",chan->helper);
    if (chan->helper != NULL)
    {
        //pthread_cancel(chan->helper);
        pthread_join(chan->helper,NULL);
    }

    printf("c->fifo count %d\n", gdf_count(c->fifo));

    while (gdf_count(c->fifo) >0)
    {
        long int *buf = NULL;
        if (gdf_get(c->fifo,&buf) < 0)
            break;

        printf("got buf %p count now %d\n",buf, gdf_count(c->fifo));

        if (buf != NULL)
            free(buf);
    }
    printf("c->fifo is empty\n");
    printf("c->fifo_aux count %d\n", gdf_count(c->fifo_aux));
    while (gdf_count(c->fifo_aux) >0)
    {
        long int *buf = NULL;
        if (gdf_get(c->fifo_aux,&buf) < 0)
            break;
        printf("got buf %p count now %d\n",buf, gdf_count(c->fifo_aux));
        if (buf != NULL)
            free(buf);
    }
    gdf_free(c->fifo);
    gdf_free(c->fifo_aux);


    free(chan);

    printf("GCHANNEL_CLOSE_SO- DONE\n");
    return 0;
}
