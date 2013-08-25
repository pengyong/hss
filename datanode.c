/*****************************************************************************
* Copyright		        : 
* Version		        : 0.1
* File Name		        : datanode.c
* Description	        : The datanode
* Author                : pengyong
* Creation              : 2013-4-24
* Modification	        : 2013-5-12
******************************************************************************/

#include <stdio.h>
#include <string.h>
#include <sys/types.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <errno.h>
#include <stdlib.h>
#include <arpa/inet.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/signal.h>
#include <pthread.h>
#include <unistd.h>
#include "def.h"
#include "global.h"
#include "mysql/mysql.h"

static uint32_t	m_capacity	=	(1<<20)*100; //100M
static uint32_t	m_used_space=	0;
static uint32_t	m_free_space=	(1<<20)*100;
uint16_t read_port	= 3001;
uint16_t write_port = 3002;
uint16_t healer_port= 3003;
uint16_t server_port= 30004;
int healerfd;
int datawritefd;
int datareadfd;

MYSQL *node_conn;

void print_help()
{
	printf("usage:./node -r read_port -w write_port -c capacity -f data_path -h\n");
	printf("\n");
}

int need_to_forward_write(int *pos , struct DataAddrList AddrList)
{
	int foundlocal = 0;
	int need_to_forward = 0;

	int index;
	uint16_t my_write_port = write_port;
	char my_address[16] = "127.0.0.1";
	for(index = 0 ; index < 3 ; index++){
		if(strcmp(my_address,AddrList.addr_list[index].address)==0 && my_write_port == AddrList.addr_list[index].write_port){
			*pos = index;
			foundlocal = 1;
			continue;
		}

		if(foundlocal == 1){
			need_to_forward = 1;
			break;
		}
	}
	return need_to_forward;
}

int handle_write(int fd , struct DataMsg msg)
{
	/*
	　　接收数据，写入；
　　若写入失败，将失败通知发起连接方（Client或DataNode）；
　　判定是否写向下一个DataNode；
　　若是，建立与下一个DataNode连接，写入数据，同时等待结果返回；
　　写入成功，且下一个写入的返回成功，则更新mysql上信息（objid,offset）；
	*/
	char objid[OBJIDLENGTH];
	bzero(objid,OBJIDLENGTH);
	strncpy(objid,msg.objid,OBJIDLENGTH);
	uint32_t objsize = msg.objsize;
	char *databuf = (char*)malloc(objsize*sizeof(char));
	strncpy(databuf,msg.data,objsize);
	lseek(datawritefd,0,SEEK_END);
	off_t objoffset = lseek(datawritefd,0,SEEK_CUR);
	if(write(datawritefd,databuf,objsize) == -1){
		HS_ERROR("write to storage failed! objid=%s,objsize=%d\n",msg.objid,objsize);
		struct ResponseMsg rmsg;
		rmsg.command = COMMAND_DATANODE_WRITE;
		rmsg.flag = -1;
		i_send(fd,(void*)&rmsg,sizeof(rmsg));
		return -1;
	}else{
		m_used_space += objsize;
		m_free_space -= objsize;
	}
	
	int forward = 0;
	int mypos = -1;
	forward = need_to_forward_write(&mypos , msg.data_list);
	printf("[handle_write:forward=%d,mypos=%d\n",forward,mypos);
	if(forward == 1){
		HS_DEBUG("write data to next node,ip=%s,write_port=%d\n",msg.data_list.addr_list[mypos+1].address,msg.data_list.addr_list[mypos+1].write_port);
		int next_datanode_fd;
		struct sockaddr_in datanode_addr;
		next_datanode_fd = i_socket(AF_INET,SOCK_STREAM,0);
		bzero(&datanode_addr,sizeof(datanode_addr));
		datanode_addr.sin_family=AF_INET;
		datanode_addr.sin_port = htons(msg.data_list.addr_list[mypos+1].write_port);
		datanode_addr.sin_addr.s_addr = inet_addr(msg.data_list.addr_list[mypos+1].address);
		i_connect(next_datanode_fd,(struct sockaddr*)&datanode_addr,sizeof(datanode_addr));
		i_send(next_datanode_fd,(void*)&msg,sizeof(msg));

		struct ResponseMsg rmsg;
		bzero(&rmsg,sizeof(rmsg));
		i_recv(next_datanode_fd,(void*)&rmsg,sizeof(rmsg));
		if(rmsg.command == COMMAND_DATANODE_WRITE && rmsg.flag == 0){
			i_send(fd,(void*)&rmsg,sizeof(rmsg));
		}else{
			rmsg.flag = -1;
			i_send(fd,(void*)&rmsg,sizeof(rmsg));
			return -1;
		}
	}else{
		HS_DEBUG("write data to local over!\n");
		struct ResponseMsg rmsg;
		rmsg.command = COMMAND_DATANODE_WRITE;
		rmsg.flag = 0;
		i_send(fd,(void*)&rmsg,sizeof(rmsg));
	}


	/* update the sql information 
	 * datatable(objid,objsize,del_flag)
	 * indextable(objid,ip,read_port,write_port,objoffset)
	 * nodetable(ip,read_port,write_port,capacity,used_space,free_space)
	 */
	int int_ip = convert_ip_to_int("127.0.0.1");
	char sql_1[SQLLENGTH],sql_2[SQLLENGTH],sql_3[SQLLENGTH];
	bzero(sql_1,SQLLENGTH);
	bzero(sql_2,SQLLENGTH);
	bzero(sql_3,SQLLENGTH);
	sprintf(sql_1,"insert into datatable values('%s',%d,0)",objid,objsize);
	sprintf(sql_2,"insert into indextable values('%s',%d,%d,%d,%d)",objid,int_ip,read_port,write_port,objoffset);
	sprintf(sql_3,"update nodetable set used_space = %d , free_space = %d where ip = %d and read_port=%d and write_port = %d",m_used_space,m_free_space,int_ip,read_port,write_port);

	if(mypos == 0){
		if(mysql_query(node_conn,sql_1)){
			HS_ERROR("insert meta[datatable] information into mysql failed! error=%s\n",mysql_error(node_conn));
			return -1;
		}else{
			HS_DEBUG("insert meta[datatable] information into mysql sucess!\n");
		}
	}
	
	if(mysql_query(node_conn,sql_2)){
		HS_ERROR("insert meta[indextable] information into mysql failed! error=%s\n",mysql_error(node_conn));
		return -1;
	}else{
		HS_DEBUG("insert meta[indextable] information into mysql sucess!objid=%s,objsize=%d,objoffset=%d\n",objid,objsize,objoffset);
	}
	
	if(mysql_query(node_conn,sql_3)){
		HS_ERROR("update nodetable used_space and free_space failed! error=%s\n",mysql_error(node_conn));
		return -1;
	}else{
		HS_ERROR("update nodetable used_space and free_space sucess,now used_space=%d,free_space=%d!\n",m_used_space,m_free_space);
	}
	return 0;
}

int handle_read(int fd , struct DataMsg msg)
{
	/* get meta information from client and read data & response*/
	struct MetaInfo meta;
	memcpy(&meta,msg.data,sizeof(meta));
	uint32_t objsize = meta.size[meta.index];
	uint32_t objoffset = meta.offset[meta.index];
	HS_DEBUG("DataNode read data,objsize=%d,objoffset=%d\n",objsize,objoffset);

	char *databuf = (char*)malloc(objsize*sizeof(char));
	int nbytes;
	lseek(datareadfd,objoffset,SEEK_SET);
	if((nbytes = read(datareadfd,databuf,objsize)) == -1){
		HS_ERROR("DataNode read from file failed!\n");
		struct DataMsg readmsg;
		bzero(&readmsg,sizeof(readmsg));
		readmsg.command == COMMAND_DATANODE_READ_FAILED;
		i_send(fd,(void*)&readmsg,sizeof(readmsg));
	}else{
		HS_DEBUG("DataNode read data from file sucess!\n");
		struct DataMsg readmsg;
		bzero(&readmsg,sizeof(readmsg));
		readmsg.command = COMMAND_DATANODE_READ;
		readmsg.objsize = objsize;
		strncpy(readmsg.data , databuf , objsize);
		i_send(fd,(void*)&readmsg,sizeof(readmsg));
	}
}

int handle_remote_write(struct DataMsg msg)
{

}

void on_read(int fd , short ev , void *arg)
{
	LIBEVENT_THREAD *me=arg;
	struct DataMsg msg;
	bzero(&msg,sizeof(msg));

	uint32_t nbytes = i_recv(fd,(void*)&msg , sizeof(msg));
	if(nbytes == sizeof(struct DataMsg)){
		switch(msg.command){
			case COMMAND_DATANODE_READ:{
				handle_read(fd,msg);
				break;
			}
			case COMMAND_DATANODE_WRITE:{
				handle_write(fd,msg);
				break;
			}
			case COMMAND_DATANODE_REMOTE_WRITE:{
				handle_remote_write(msg);
				break;
			}
			default:{
				HS_ERROR("on_read:unknow command!command=%d\n",msg.command);
				break;
			}
		}
	}else{
		HS_ERROR("read zero,maybe the connection broken!\n");
		close(fd);
	}
	
	
}

void on_accept(int fd,  short ev , void *arg)
{
	LIBEVENT_THREAD *work_threads = (LIBEVENT_THREAD*)arg;
	static int index=0;
	struct conn_queue_item *client_item=(struct conn_queue_item*)calloc(1,sizeof(struct conn_queue_item));
	int client_fd;
	struct sockaddr_in client_addr;
	socklen_t client_len=sizeof(client_addr);
	
	client_fd = accept(fd, (struct sockaddr*)&client_addr , &client_len);
	if(client_fd == -1){
		warn("accept failed");
		return ;
	}
	
	if(setnonblock(client_fd) < 0){
		warn("failed to set client sock non-blocking");
		return;
	}
	
	client_item->fd=client_fd;
	int k=index%CORE_COUNT;
	cq_push(work_threads[k].new_conn_queue,client_item);
	if(write(work_threads[k].notify_send_fd,"",1)!=1){
		perror("writeing to thread notify pipe");
	}
	HS_DEBUG("index:%d\n",k);
	HS_DEBUG("main thread accepted connection from %s\n",inet_ntoa(client_addr.sin_addr));
	index++;
}

void thread_libevent_process(int fd , short which , void *arg)
{
	LIBEVENT_THREAD *me=arg;
	struct conn_queue_item  *client_item;
	int client_fd;
	char buf[1];
	
	if(read(fd,buf,1)!=1){
		perror("can't read from libevent pipe!");
		exit(1);
	}
	
	client_item = cq_pop(me->new_conn_queue);
	
	if(client_item != NULL){
		client_fd=client_item->fd;
		event_set(&me->event,client_fd,EV_READ|EV_PERSIST,on_read,(void*)me);
		event_base_set(me->base,&me->event);
		event_add(&me->event,NULL);
	}
}
	
void create_worker(void *(*func)(void*),void *arg)
{
	pthread_t thread;
	pthread_attr_t attr;
	int ret;
	
	pthread_attr_init(&attr);
	if((ret=pthread_create(&thread,&attr,func,arg))!=0){
		fprintf(stderr,"can't create thread:%s\n",strerror(ret));
		exit(1);
	}
}

void *worker_libevent(void *arg)
{
	LIBEVENT_THREAD *me = arg;
	event_base_loop(me->base,0);
	return NULL;
}

int SocketSetUp(uint16_t port);

void *socket_run(void *arg)
{
	uint16_t port = *(uint16_t*)arg;
	SocketSetUp(port);
	return NULL;
}

LIBEVENT_THREAD * InitLibevent()
{
	LIBEVENT_THREAD *m_threads = NULL;
	m_threads=calloc(CORE_COUNT,sizeof(LIBEVENT_THREAD));
	
	int i;
	for(i=0 ; i<CORE_COUNT ; i++){
		int fds[2];
		if(pipe(fds)){
			perror("can't create notify pipe");
			exit(1);
		}
		m_threads[i].notify_receive_fd = fds[0];
		m_threads[i].notify_send_fd = fds[1];
		
		m_threads[i].base = event_init();
		if(!m_threads[i].base){
			perror("can't allocate event_base");
			exit(1);
		}
		event_set(&m_threads[i].notify_event , m_threads[i].notify_receive_fd,
					EV_READ|EV_PERSIST , thread_libevent_process , &m_threads[i]);
		event_base_set(m_threads[i].base,&m_threads[i].notify_event);
		event_add(&m_threads[i].notify_event,NULL);
		m_threads[i].new_conn_queue = malloc(sizeof(struct conn_queue));
		cq_init(m_threads[i].new_conn_queue);
	}
	
	for(i=0 ; i<CORE_COUNT ; i++){
		create_worker(worker_libevent , &m_threads[i]);
	}
	return m_threads;
}

void *DataNodeRegister(void *arg) {
	struct DataNodeInfo info = *(struct DataNodeInfo*)arg;
	struct HealerMsg	msg;
	msg.command			=	COMMAND_DATANODE_REGISTER;
	msg.content_length	=	sizeof(info);
	memcpy(msg.content,&info,sizeof(info));

	i_send(healerfd,(void*)&msg,sizeof(msg));

}

void *DataNodeHeartBeat(void *arg) {
	struct HealerMsg	msg;
	bzero(&msg,sizeof(struct HealerMsg));
	msg.command			=	COMMAND_DATANODE_HEARTBEAT;
	sprintf(msg.content,"127.0.0.1#%d#%d",read_port,write_port);
	
	while(1){
		i_send(healerfd,(void*)&msg,sizeof(msg));
		sleep(60);
	}
}

int SocketSetUp(uint16_t port)
{
	int i,listen_fd;
	struct sockaddr_in server_addr;
	struct event *server_event;
	int reuseaddr_on =1;
	uint16_t m_port = port;
	struct event_base *main_base=NULL;
	LIBEVENT_THREAD *work_threads;

	main_base=event_init();
	if (!main_base) {
		fprintf(stderr, "Couldn't create an event_base: exiting\n");
		return;
	}
	work_threads=InitLibevent();
	
	listen_fd = socket(AF_INET , SOCK_STREAM, 0);
	if(listen_fd < 0){
		err(1,"listen failed!");
		return -1;
	}
	if(setsockopt(listen_fd , SOL_SOCKET , SO_REUSEADDR , &reuseaddr_on,sizeof(reuseaddr_on))==-1){
		err(1,"setsockopt failed");
		return -1;
	}
	
	memset(&server_addr,0,sizeof(server_addr));
	server_addr.sin_family=AF_INET;
	server_addr.sin_addr.s_addr=INADDR_ANY;
	server_addr.sin_port=htons(m_port);
	if(bind(listen_fd,(struct sockaddr*)&server_addr,sizeof(server_addr))<0){
		err(1,"bind failed");
		return -1;
	}
	if(listen(listen_fd,BACKLOG)<0){
		err(1,"listen failed");
		return -1;
	}
	
	if(setnonblock(listen_fd)<0){
		err(1,"failed to set the server socket to non-blocking");
		return -1;
	}

	server_event=event_new(main_base,listen_fd,EV_READ|EV_PERSIST,on_accept,(void*)work_threads);
	if(server_event){
		event_add(server_event,NULL);
	}else{
		HS_DEBUG("event_new failed!\n");
		exit(1);
	}

	event_base_dispatch(main_base);
	event_base_free(main_base);
	
	return 0;
}

int sql_init()
{
	char *sql_server = "172.16.0.254";
	char *sql_user = "root";
	char *sql_password = "py";
	char *sql_database = "hs";

	node_conn = mysql_init(NULL);
	if(!mysql_real_connect(node_conn,sql_server,sql_user,sql_password,sql_database,0,NULL,0)){
		HS_ERROR("mysql init failed! error:%s\n",mysql_error(node_conn));
		return -1;
	}
	return 0;
}

int data_file_init(char *path)
{
	datawritefd = open(path,O_WRONLY|O_CREAT|O_APPEND);
	datareadfd = open(path,O_RDONLY);
	if(datareadfd > 0 && datawritefd > 0){
		return 0;
	}else{
		HS_ERROR("data file open failed!error:\n",strerror(errno));
		return -1;
	}
}

int main(int argc , char **argv) 
{
	char m_data_path[100];
	char optchar;
	while((optchar = getopt(argc,argv,"hr:w:c:f:")) != -1){
		switch(optchar){
			case 'r':{
				read_port = atoi(optarg);
				break;
			}
			case 'w':{
				write_port = atoi(optarg);
				break;
			}
			case 'c':{
				m_capacity = atoi(optarg);
				m_free_space = m_capacity;
				break;
			}
			case 'f':{
				strcpy(m_data_path,optarg);
				break;
			}
			case 'h':{
				print_help();
				return 0;
			}
			default:{
				print_help();
				return 0;
			}
		}
	}

	if(sql_init() == -1){
		return -1;
	}else{
		HS_DEBUG("sql init sucess!\n");
	}

	
	if(data_file_init(m_data_path) == -1){
		return -1;
	}else{
		HS_DEBUG("data file %s init sucess!\n",m_data_path);
	}
	struct sockaddr_in seraddr;
	
	healerfd =	i_socket(AF_INET,SOCK_STREAM,0);
	bzero(&seraddr,sizeof(struct sockaddr_in));
	seraddr.sin_family=AF_INET;
	seraddr.sin_port=htons(server_port);
	seraddr.sin_addr.s_addr=INADDR_ANY;
	i_connect(healerfd,(struct sockaddr*)&seraddr,sizeof(seraddr));

	struct DataNodeInfo data_info;
	strcpy(data_info.address,	"127.0.0.1");
	data_info.capacity	=	m_capacity;
	data_info.free_space=	m_free_space;
	data_info.used_sapce=	m_used_space;
	data_info.read_port	=	read_port;
	data_info.write_port=	write_port;
	data_info.healer_port=	healer_port;
	create_worker(DataNodeRegister, (void*)&data_info);
	
	create_worker(DataNodeHeartBeat, NULL);

	create_worker(socket_run,&read_port);
	create_worker(socket_run,&write_port);

	while(1){}
	return 0;

}


