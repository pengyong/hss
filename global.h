#ifndef _I_
#define _I_

#include <sys/types.h>
#include <sys/socket.h>
#include <unistd.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <stdio.h>
#include <stdlib.h>
#include <errno.h>
#include <netdb.h>
#include <stdarg.h>
#include <string.h>
#include <net/if.h>
#include <sys/ioctl.h>
#include <pthread.h>
#include <sys/time.h>
#include <signal.h>
#include <sys/stat.h>
#include <fcntl.h>
#include "def.h"	
#include "libevent-2.0.20-stable/event.h"

struct conn_queue_item{
	int fd;
	struct conn_queue_item *next;
};

struct conn_queue{
	struct conn_queue_item *head;
	struct conn_queue_item *tail;
};
	
typedef struct{
	pthread_t thread_id;
	struct event_base *base;
	struct event event;
	struct event notify_event;
	int notify_receive_fd;
	int notify_send_fd;
	struct conn_queue *new_conn_queue;
}LIBEVENT_THREAD;

struct DataNodeInfo{
	uint32_t	capacity;
	uint32_t	used_sapce;
	uint32_t	free_space;
	char		address[16];
	uint16_t	read_port;
	uint16_t	write_port;
	uint16_t	healer_port;
};

#define CONTENTLENGTH	1024 //sizeof(struct DataNodeInfo)
struct HealerMsg{
	uint16_t				command;
	//struct DataNodeInfo		datanode_info;
	uint32_t 				content_length;
	char 					content[CONTENTLENGTH];
};

struct DataAddr {
	char		address[16];
	uint16_t	read_port;
	uint16_t	write_port;
};

struct DataAddrList{
	struct DataAddr		addr_list[3];
};

struct DataMsg{
	uint16_t				command;
	struct DataAddrList		data_list;
	uint32_t				objsize;
	char					objid[OBJIDLENGTH];
	char					data[DATALENGTH];
};

struct MetaInfo{
	uint16_t				index;
	uint32_t				size[3];
	uint32_t				offset[3];
};

struct ResponseMsg{
	uint16_t				command;
	int						flag; //0--sucess ,-1 failed
};

void cq_init(struct conn_queue *cq)
{
	cq->head = NULL;
	cq->tail = NULL;
}

struct conn_queue_item *cq_pop(struct conn_queue *cq)
{
	struct conn_queue_item *item;
	item=cq->head;
	if(item != NULL){
		cq->head = item->next;
		if(cq->head == NULL){
			cq->tail = NULL;
		}
	}
	return item;
}

void cq_push(struct conn_queue *cq , struct conn_queue_item *item)
{
	item->next=NULL;
	if(cq->tail == NULL){
		cq->head = item;
	}else{
		cq->tail->next = item;
	}
	cq->tail=item;
}

int setnonblock(int fd)
{
	int flags;
	
	flags = fcntl(fd,F_GETFL);
	if(flags < 0){
		return flags;
	}
	flags |= O_NONBLOCK;
	if(fcntl(fd,F_SETFL,flags)<0){
		return -1;
	}
	
	return 0;
}


int i_socket(int domain , int type , int protocol)
{
	int fd;
	if((fd=socket(domain,type,protocol))==-1){
		perror("create socket error");
		exit(1);
	}
	return(fd);
}

int i_connect(int fd, struct sockaddr *serv , int addrlen)
{
	if(connect(fd,serv,addrlen)==-1){
		perror("connect error");
		exit(1);
	}
	return(0);
}

int i_bind(int fd , struct sockaddr *myaddr , int addrlen)
{
	if(bind(fd, myaddr,addrlen)==-1){
		perror("bind error");
		exit(1);
	}
	return(0);
}

int i_listen(int fd , int backlog)
{
	if(listen(fd, backlog)==-1){
		perror("listen error");
		exit(1);
	}
	return(0);
}

int i_accept(int fd , struct sockaddr *addr , int *addrlen)
{
	int d;
	printf("in accept..\n");
	if((d=accept(fd, addr, addrlen))==-1){
		perror("accept error");
		exit(1);
	}
	return(d);
}

int i_send(int fd , void *msg , int len)
{
	int sendlen;
	if((sendlen=send(fd,msg,len,0))==-1){
		perror("send msg error");
		exit(1);
	}
	return(sendlen);
}

int i_recv(int fd , void *buf , int len)
{
	int recvlen;
	if((recvlen=recv(fd,buf,len,0))==-1){
		perror("recv msg error");
		exit(1);
	}
	return(recvlen);
}

char *convert_int_to_ip(char *s_ip)
{
	int int_ip = atoi(s_ip);
	struct in_addr addr;
	addr.s_addr = int_ip;
	return inet_ntoa(addr);
}

int convert_ip_to_int(char *ip)
{
	struct in_addr addr;
	addr.s_addr = inet_addr(ip);
	return addr.s_addr;
}


#endif

