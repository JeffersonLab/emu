
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
 *      emu  - gchannel_cmsg.c
 *
 * 	Implementation of the gchannel API using TCP sockets
 *
 *----------------------------------------------------------------------------*/

#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include "cMsg.h"
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

typedef  struct cmsg_transport_st *cmsg_transport;

typedef struct cmsg_transport_st
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
    void *domainId;
    void *unSubHandle;
    gll_li channels;
}
cmsg_transport_ty;

typedef struct cmsg_channel_st *cmsg_channel;

typedef struct cmsg_channel_st
{
    gchannel c;
    char *name;
    int run;
    int active;
    int owner;
    int32_t dataSock;
    pthread_t helper;
    cmsg_transport transp;
}
cmsg_channel_ty;

cmsg_channel cmsg_find_channel(cmsg_transport trans, char *name)
{
    if ((trans == NULL) || (name == NULL))
        return NULL;
    cmsg_channel chan = NULL;
    gll_li channels = trans->channels;

    for (gll_el el = gll_get_first(channels);el!=NULL;el = gll_get_next(el))
    {
        cmsg_channel tmp = (cmsg_channel) gll_get_data(el);

        if (strcmp(name,tmp->name) == 0 )
        {
            chan = tmp;
            break;
        }
    }

    return chan;
}

extern "C" int cmsg_gchannel_net_send(int socket, char *data, int length)
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

extern "C" void cmsg_transport_callback(void *msg, void *arg)
{
    cmsg_transport transp = (cmsg_transport) arg;
    char *type;

    cMsgGetType(msg, (const char**) &type);

    printf("cMsg %s %s, reply %s:%d\n",type,transp->name,transp->host,transp->port);

    if (strcmp(type,"get_socket") == 0)
    {
        int req = 0;
        cMsgGetGetRequest(msg,&req);
        if (req)
        {
            void *response = cMsgCreateResponseMessage(msg);

            cMsgSetSubject(response,transp->name);
            cMsgSetType(response,"socket_ack");
            cMsgSetText(response,transp->host);
            cMsgSetUserInt(response,transp->port);
            cMsgSend(transp->domainId,response);
            //cMsgFlush(transp->domainId, &myTimeout);
            cMsgFreeMessage(&response);
        }
    }
    cMsgFreeMessage(&msg);
}

extern "C" void *gchannel_read_helper_cmsg(void *arg);

extern "C" void *gchannel_accept_helper_cmsg(void *arg)
{
    gtransport m = (gtransport) arg;
    cmsg_transport transp = (cmsg_transport) m->hidden;
    int len = 0;
    transp->listen_sock = socket(PF_INET, SOCK_STREAM, 0);
    int window_size = 40000;
    int flag = 1;
    struct hostent *hostentry;

    if(-1 == transp->listen_sock)
    {
        printf("can not create socket");
        transp->listen_sock = -1;
        goto gchannel_accept_helper_cmsg_end;
    }

    setsockopt(transp->listen_sock, SOL_SOCKET, SO_SNDBUF,
               (char *) &window_size, sizeof(window_size));


    setsockopt(transp->listen_sock, IPPROTO_TCP, TCP_NODELAY,
               (char *) &flag, sizeof(flag));


    printf("hostname is %s\n", transp->host);

    hostentry = gethostbyname(transp->host);

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
        goto gchannel_accept_helper_cmsg_end;
    }
    len = sizeof(transp->listenAddr);
    getsockname(transp->listen_sock,(struct sockaddr*) &transp->listenAddr, (socklen_t*) &len);

    transp->port = ntohs(transp->listenAddr.sin_port);

    if (-1 == listen(transp->listen_sock, 100))
    {
        printf("error listen failed");
        close(transp->listen_sock);
        transp->listen_sock = -1;
        goto gchannel_accept_helper_cmsg_end;

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

            goto gchannel_accept_helper_cmsg_end;
        }
        uint32_t magic;
        read(i32ConnectFD,&magic,4);
        printf("got %08x\n", ntohl(magic));
        char l;
        read(i32ConnectFD,&l,1);
        printf("got a connection %d\n",i32ConnectFD);
        printf("length of name is %d\n",l);
        int actual = read(i32ConnectFD,tmp,l);
        printf("read %d bytes %s\n", actual,tmp);

        printf("[%s]\n",tmp);
        cmsg_channel chan = cmsg_find_channel(transp,tmp);
        if (chan != NULL)
        {
            printf("associating socket %d with channel %s\n", i32ConnectFD, chan->name);
            chan->dataSock = i32ConnectFD;
            sprintf(tmp,"%s channel helper",chan->name);
            pthread_create(&chan->helper,NULL, gchannel_read_helper_cmsg, (void *) chan);
        }
        else
        {
            printf("No data channel named %s was found\n", tmp);
        }
    }
gchannel_accept_helper_cmsg_end:
    pthread_exit(NULL);
    return NULL;

}


extern "C" int gchannel_create_transport_cmsg(gtransport m)
{
    char tmp[1000];
    char *UDL = "cMsg:cMsg://antares:7030/cMsg/test";
    cmsg_transport transp = (cmsg_transport) malloc(sizeof(cmsg_transport_ty));

    bzero (transp,sizeof(cmsg_transport_ty));
    m->hidden = transp;

    transp->name = strdup(m->name);
    transp->capacity = 2;
    transp->length = 32000000;
    transp->owner = true;
    transp->port=7030;

    char hostname[1000];
    gethostname(hostname,1000);

    transp->host=strdup(hostname);

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

        value = gchannel_getTransportAttr(m, "UDL");

        if (value != NULL)
            UDL = strdup(value);

    }

    sprintf(tmp,"%s accept helper",transp->name);
    pthread_create(&transp->accept_helper, NULL, gchannel_accept_helper_cmsg, (void *) m);

    cMsgSetDebugLevel(CMSG_DEBUG_ERROR );

    int err;

    err = cMsgConnect(UDL, "gchannel_create_transport", "data transport", &transp->domainId);


    if (err != CMSG_OK)
    {
        printf("cMsgConnect: %s\n",cMsgPerror(err));
    }
    /* start receiving messages */
    cMsgReceiveStart(transp->domainId);

    /* subscribe */
    printf("cMsgSubscribe: %s %s\n", transp->name, "get_socket");
    err = cMsgSubscribe(transp->domainId, transp->name, "get_socket", cmsg_transport_callback, transp, NULL, &transp->unSubHandle);
    if (err != CMSG_OK)
    {
        printf("cMsgSubscribe: %s\n",cMsgPerror(err));
    }

    return 0;
}

extern "C" void *gchannel_read_helper_cmsg(void *arg)
{
    cmsg_channel chan = (cmsg_channel) arg;
    gchannel c = chan->c;

    struct timespec wait;

    wait.tv_nsec = 0;
    wait.tv_sec = 1;

    chan->active = TRUE;
    printf("gchannel_read_helper_cmsg : thread starts\n");

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

                goto gchannel_read_helper_cmsg_end;
            }
        }
        while (status == ETIMEDOUT);

        int bytes = recv(chan->dataSock,&in_data->length,sizeof(in_data->length),MSG_WAITALL);

        if (bytes != sizeof(in_data->length))
        {
            printf("length not 4 bytes long! it's %d\n",bytes);
            goto gchannel_read_helper_cmsg_end;
        }

        // Always transmit lengths in network byte order so translate back.
        in_data->length = ntohl(in_data->length);

        bytes = recv(chan->dataSock,&in_data->data,in_data->length*sizeof(int32_t),MSG_WAITALL);

        do
        {
            pthread_testcancel();
            // gdf_get will return a status of ETIMEDOUT after 0.25 seconds.
            status = gdf_put(c->fifo, (long int *) in_data);

            if (chan->run == FALSE)
            {

                goto gchannel_read_helper_cmsg_end;
            }
        }
        while (status == ETIMEDOUT);
        char ack = 0xaa;

        cmsg_gchannel_net_send(chan->dataSock, (char *) &ack, sizeof(ack));

    }
gchannel_read_helper_cmsg_end:
    chan->run = FALSE;
    chan->active = FALSE;
    pthread_exit(NULL);
    return NULL;
}

extern "C" int gchannel_create_cmsg(gchannel c)
{
    char tmp[100];

    cmsg_transport transp = (cmsg_transport) c->transport->hidden;
    cmsg_channel chan = (cmsg_channel) malloc(sizeof(cmsg_channel_ty));
    bzero(chan, sizeof(cmsg_channel_ty));

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

extern "C" int gchannel_open_transport_cmsg(gtransport m)
{

    struct timespec time_to_wait;
    time_to_wait.tv_nsec = 0;
    time_to_wait.tv_sec = 4;

    cmsg_transport transp = (cmsg_transport) malloc(sizeof(cmsg_transport_ty));

    bzero (transp,sizeof(cmsg_transport_ty));
    m->hidden = transp;

    transp->name = strdup(m->name);
    transp->owner = false;
    transp->port = 7030;
    transp->host = strdup("antares");
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

	char *UDL = "cMsg://localhost:7030/cMsg/test";

	cMsgSetDebugLevel(CMSG_DEBUG_ERROR);

	int err = cMsgConnect(UDL, "gchannel_open_transport", "data transport", &transp->domainId);
	if (err != CMSG_OK)
    {
        printf("cMsgConnect: %s\n",cMsgPerror(err));
        return -1;
    }

    /* start receiving messages */
    cMsgReceiveStart(transp->domainId);

    void *reply,*msg;
    timespec tv;
    tv.tv_nsec = 0;
    tv.tv_sec = 5;

    msg = cMsgCreateMessage();

    printf("sending : subject, %s \n",transp->name);
    cMsgSetSubject(msg,transp->name);
    cMsgSetType(msg,"get_socket");
    cMsgSetText(msg,"text");

	err = cMsgSendAndGet(transp->domainId,msg,&tv,&reply);
	if (err == CMSG_TIMEOUT) {
              printf("TIMEOUT in GET\n");
              exit(-1);
          }

	if(err !=CMSG_OK) {
   	 printf("cMsgSendAndGet: %s\n",cMsgPerror(err));
        exit(-1);
	}
    printf("here\n");
    cMsgGetText(reply,(const char **)&transp->listen_host);
    cMsgGetUserInt(reply,&transp->port);
    cMsgFreeMessage(&msg);
    printf("Here at the end\n");
    return 0;
}

extern "C" void *gchannel_send_helper_cmsg(void *arg)
{
    gchannel c = (gchannel) arg;

    cmsg_channel chan = (cmsg_channel) c->hidden;

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

        //printf("gchannel_send_helper_cmsg thread wait for data\n");
        do
        {
            // gdf_get will return a status of ETIMEDOUT after 0.25 seconds.
            status = gdf_get(c->fifo, (long int **)&out_data);


            if (!chan->run)
            {
                printf("chan-> run is false\n");
                goto gchannel_send_helper_cmsg_end;
            }
        }
        while (status == ETIMEDOUT);

        //printf("gchannel_send_helper_cmsg: %s %d\n",c->name,out_data->length*sizeof(int32_t));

        int32_t net_length = htonl(out_data->length);

        if ( cmsg_gchannel_net_send(chan->dataSock, (char *) &net_length, sizeof(net_length)) < 0)
        {
            printf("gchannel_net_send send length failed\n");
            goto  gchannel_send_helper_cmsg_end;
        }

        if ( cmsg_gchannel_net_send(chan->dataSock, (char *) &out_data->data, out_data->length*sizeof(int32_t)) < 0)
        {
            printf("gchannel_net_send send length failed\n");
            goto  gchannel_send_helper_cmsg_end;
        }

        char ack = 0;

        int recvd = read(chan->dataSock,&ack,sizeof(ack));

        if ((recvd !=sizeof(ack)) || (ack != (char) 0xaa))
        {
            printf("recvd is %d ack is %02x\n",recvd, ack);
            goto gchannel_send_helper_cmsg_end;
        }

        //printf("gchannel_send_helper_cmsg: recvd ack %d\n", recvd);
        do
        {
            pthread_testcancel();
            // gdf_get will return a status of ETIMEDOUT after 0.25 seconds.
            status = gdf_put(c->fifo_aux, (long int *) out_data);

            if (!chan->run)
            {
                printf("chan-> run is false after gdf_put\n");
                goto gchannel_send_helper_cmsg_end;
            }
        }
        while (status == ETIMEDOUT);
        out_data = NULL;
    }

gchannel_send_helper_cmsg_end:
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
    printf("gchannel_send_helper_cmsg_end %d\n",chan->run);
    chan->run = FALSE;
    chan->active = FALSE;
    pthread_exit(NULL);
    return (NULL);
}

extern "C" int gchannel_open_cmsg(gchannel c)
{
    cmsg_transport transp = (cmsg_transport) c->transport->hidden;

    cmsg_channel chan = (cmsg_channel) malloc(sizeof(cmsg_channel_ty));
    bzero(chan, sizeof(cmsg_channel_ty));
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
    uint32_t magic = htonl(0xC0DA2008);
    cmsg_gchannel_net_send(chan->dataSock,(char *) &magic,4);
    char l = strlen(c->name);
    cmsg_gchannel_net_send(chan->dataSock,&l,1);
    cmsg_gchannel_net_send(chan->dataSock,c->name,strlen(c->name));

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
    pthread_create(&chan->helper, NULL, gchannel_send_helper_cmsg, (void *) c);

    gll_add_el(transp->channels, (void *) chan);
    return 0;
}

extern "C" int gchannel_send_cmsg(gchannel_hdr *buffer)
{
    return gdf_put(buffer->channel->fifo,(long *) buffer);
}

extern "C" int gchannel_receive_cmsg(gchannel channel, gchannel_hdr *buffer)
{
    //printf("gchannel_receive_cmsg : waiting on fifo for data\n");
    int status = gdf_get(channel->fifo, (long **)buffer);
    //printf("gchannel_receive_cmsg : gdf_get returns %d %p \n",status,*buffer);
    return status;
}

extern "C" int gchannel_close_transport_cmsg(gtransport m)
{
    printf("GTRANSPORT_CLOSE_cmsg\n");
    cmsg_transport transp = (cmsg_transport) m->hidden;

    if (transp->owner)
    {
        cMsgUnSubscribe(transp->domainId, transp->unSubHandle);
        cMsgDisconnect(&transp->domainId);
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

    }
    else
    {
        gll_delete_li(transp->channels);
        free(transp->host);
        free(transp->listen_host);
    }
    free(transp->name);
    free(transp);
    printf("GTRANSPORT_CLOSE_cmsg Done\n");

    return 0;
}

extern "C" int gchannel_allocate_cmsg(gchannel c, gchannel_hdr **data)
{
    int status;
    cmsg_channel chan = (cmsg_channel) c->hidden;
    //printf("gchannel_allocate_cmsg wait for data\n");
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

extern "C" int gchannel_free_cmsg(gchannel_hdr *buf)
{
    buf->length = 0;
    gdf_put(buf->channel->fifo_aux, (long *) buf);
    return 0;
}

extern "C" int gchannel_close_cmsg(gchannel c)
{
    printf("GCHANNEL_CLOSE_cmsg\n");
    cmsg_transport transp = (cmsg_transport) c->transport->hidden;
    cmsg_channel chan = (cmsg_channel) c->hidden;
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

    printf("GCHANNEL_CLOSE_cmsg- DONE\n");
    return 0;
}
