/*****************************************************************************
* Copyright		        : 
* Version		        : 0.1
* File Name		        : master.c
* Description	        : 
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
#include "def.h"
#include "global.h"
#include "mysql/mysql.h"
#include "ConHash/conhash.h"

#define NODE_EXPIRE		180		//(s)
MYSQL *master_conn;
struct conhash_s *conhash;
struct node_s g_nodes[NODE_MAX_NUM];

struct NodeTime{
	char node_id[30];
	struct timeval lasttime;
};

struct GNodeTime{
	struct NodeTime g_node_time[NODE_MAX_NUM];
	uint32_t curr_node_num;
};
struct GNodeTime m_g_node_time;

void* check_node_expire(void *arg)
{
	while(1){
		sleep(60);
		struct timeval nowtime;
		gettimeofday(&nowtime,NULL);
		int i;
		for(i = 0 ; i < m_g_node_time.curr_node_num ; i++){
			if(nowtime.tv_sec - m_g_node_time.g_node_time[i].lasttime.tv_sec >= NODE_EXPIRE){
				HS_ERROR("It is expire time,maybe the node:%s has lost connection!\n",m_g_node_time.g_node_time[i].node_id);
			}
		}
	}
}

int handle_heartbeat(struct HealerMsg msg)
{
	HS_DEBUG("get heart beat!msg=%s\n",msg.content);
	/**
	 * when we get the node heartbeat,we also update the node time
	 */
	char msgcontent[30];
	strncpy(msgcontent,msg.content,30);
	struct timeval node_timeval;
	gettimeofday(&node_timeval,NULL);
	int i;
	for(i = 0 ; i < m_g_node_time.curr_node_num ; i++){
		if(strcmp(m_g_node_time.g_node_time[i].node_id,msgcontent) == 0){
			m_g_node_time.g_node_time[i].lasttime = node_timeval;
			break;
		}
	}
	return 0;
}

int handle_register(struct HealerMsg msg)
{
	HS_DEBUG("get register msg!\n");
	struct DataNodeInfo info;
	bzero(&info,sizeof(info));
	memcpy(&info,msg.content,sizeof(struct DataNodeInfo));
	HS_DEBUG("register information:\n");
	HS_DEBUG("capacity=%d,used_space=%d,free_space=%d,ip=%s,read_port=%d,write_port=%d\n",\
		info.capacity,info.used_sapce,info.free_space,info.address,info.read_port,info.write_port);

	/**
	 * add the node to global node time,and record the node time
	 */
	char nodetime_t[30];
	struct timeval node_timeval;
	gettimeofday(&node_timeval,NULL);
	sprintf(nodetime_t , "%s#%d#%d", info.address,info.read_port,info.write_port);
	strncpy(m_g_node_time.g_node_time[m_g_node_time.curr_node_num].node_id,nodetime_t,30);
	m_g_node_time.g_node_time[m_g_node_time.curr_node_num].lasttime = node_timeval;
	m_g_node_time.curr_node_num++;

	/* set and add the node to conhash 
	 * which based on the node ip:read_port:write_port
	 */
	static uint16_t con_node_index = 0;
	char node_str[100];
	uint32_t v_node_num = info.capacity/SIZE_PER_V_NODE;
	sprintf(node_str,"%s:%04d:%04d",info.address,info.read_port,info.write_port);
	HS_DEBUG("node_str:%s,v_node_num:%d\n",node_str,v_node_num);
	conhash_set_node(&g_nodes[con_node_index],node_str,v_node_num);
	conhash_add_node(conhash,&g_nodes[con_node_index++]);
	//printf("virtual nodes number %d\n", conhash_get_vnodes_num(conhash));
	
	/* update the info to sql */
	char sql[SQLLENGTH];
	bzero(sql,SQLLENGTH);
	int ip_int = convert_ip_to_int(info.address);
	sprintf(sql,"insert into nodetable values(%d,%d,%d,%d,%d,%d);",ip_int,
				info.read_port,info.write_port,info.capacity,info.used_sapce,info.free_space);

	if(mysql_query(master_conn,sql)){
		char _sql[SQLLENGTH];
		bzero(_sql,SQLLENGTH);
		sprintf(_sql,"update nodetable set capacity=%d,used_space=%d,free_space=%d \
					  where ip = %d and read_port=%d and write_port=%d ",info.capacity,
					  info.used_sapce,info.free_space,ip_int,info.read_port,info.write_port);
		if(mysql_query(master_conn,_sql)){
			HS_ERROR("register node information failed!\n");
		}else{
			HS_DEBUG("update node register information sucess!\n");
		}
	}else{
		HS_DEBUG("register node information sucess!\n");
	}
	return 0;
}

int handle_get_addr_list(int fd)
{
	HS_DEBUG("get addr list msg!\n");
	/* first , we just select three node from sql */
/*	char sql[SQLLENGTH];
	bzero(sql,SQLLENGTH);
	sprintf(sql,"select * from nodetable order by free_space desc");

	if(mysql_query(master_conn,sql)){
		HS_ERROR("get node list from mysql failed!\n");
	}else{
		MYSQL_RES *res;
		MYSQL_ROW row;
		res = mysql_use_result(master_conn);
		int i;
		struct DataAddrList nodelist;
		bzero(&nodelist,sizeof(struct DataAddrList));
		for(i = 0 ; i < 3 && ((row=mysql_fetch_row(res))!=NULL); i++){
			char *ip = convert_int_to_ip(row[0]);
			printf("%s,%s,%s\n",ip,row[1],row[2]);
			strncpy(nodelist.addr_list[i].address,ip,16);
			nodelist.addr_list[i].read_port = atoi(row[1]);
			nodelist.addr_list[i].write_port = atoi(row[2]);
		}
		mysql_free_result(res);

		struct HealerMsg smsg;
		bzero(&smsg,sizeof(smsg));
		smsg.command = COMMAND_CLIENT_GET_ADDR_LIST;
		smsg.content_length = sizeof(struct DataAddrList);
		memcpy(smsg.content,&nodelist,smsg.content_length);
		i_send(fd,(void*)&smsg,sizeof(smsg));
		
		HS_DEBUG("get node list from mysql sucess!\n");
	}
*/

	/* consist hashing , get the list 
	 * here,the way to get the addr list is map the client fd to conhash node
	 * as we need to know three differenet node(not virtual node),
	 * we need to map three times fd,which is fd#1,fd#2,fd#3,
	 * to make sure we store data into different node actually,
	 * we should check the map result,if two of them is the same one,
	 * we'd better map again,maybe fd#4,...
	 */
	//printf("virtual nodes number %d\n", conhash_get_vnodes_num(conhash));
	const struct node_s *node;
	char str[30];
	char node_array[3][30];
	int i=1,map_i=0;
	int is_same =0;
	while(map_i < 3 && i < 100){
		sprintf(str,"%d#%d",fd,i++);
		node = conhash_lookup(conhash,str);
		if(node){
			int k;
			for(k = 0 ; k < map_i ; k++){
				if(strcmp(node_array[k],node->iden) == 0){
					HS_DEBUG("the %s is same to %d\n",node->iden,k);
					is_same = 1;
					break;
				}else{
					is_same = 0;
				}
			}
			if(is_same == 0){
				strcpy(node_array[map_i++],node->iden);
				HS_DEBUG("[%5s] is in node [%20s],i=%d\n",str,node->iden,i);
			}
		}
	}
	if(map_i != 3){
		HS_ERROR("get hash node error!map_i=%d,i=%d\n",map_i,i);
		return -1;
	}
	

	struct DataAddrList nodelist;
	bzero(&nodelist,sizeof(struct DataAddrList));
	char *delim = ":";
	char *p;
	for(i  = 0 ; i < 3 ; i++){
		p=strtok(node_array[i],delim);
		//printf("[%d]%s  ",i,p);
		strncpy(nodelist.addr_list[i].address,p,16);
		p = strtok(NULL,delim);
		//printf("%s  ",p);
		nodelist.addr_list[i].read_port = atoi(p);
		p = strtok(NULL,delim);
		//printf("%s  \n",p);
		nodelist.addr_list[i].write_port = atoi(p);
	}
	
	struct HealerMsg smsg;
	bzero(&smsg,sizeof(smsg));
	smsg.command = COMMAND_CLIENT_GET_ADDR_LIST;
	smsg.content_length = sizeof(struct DataAddrList);
	memcpy(smsg.content,&nodelist,smsg.content_length);
	i_send(fd,(void*)&smsg,sizeof(smsg));
	
	HS_DEBUG("get node list from mysql sucess!\n");

	return 0;
}

int handle_delete(int fd , struct HealerMsg msg)
{
	char objid[OBJIDLENGTH];
	strncpy(objid,msg.content,OBJIDLENGTH);
	HS_DEBUG("get delete msg,delete %s\n",objid);

	/* set the obj table flag del, in the sql */
	char sql[SQLLENGTH];
	bzero(sql,SQLLENGTH);
	sprintf(sql,"update datatable set del_flag = 1 where objid = '%s'",objid);

	struct ResponseMsg rmsg;
	bzero(&rmsg,sizeof(rmsg));
	if(mysql_query(master_conn,sql)){
		HS_ERROR("set obj(%s) to delete failed!\n",objid);
		rmsg.command = COMMAND_CLIENT_DELETE;
		rmsg.flag = -1;
		i_send(fd,(void*)&rmsg,sizeof(rmsg));
		return 0;
	}else{
		HS_ERROR("set obj(%s) to delete sucess!\n",objid);
		rmsg.command = COMMAND_CLIENT_DELETE;
		rmsg.flag = 0;
		i_send(fd,(void*)&rmsg,sizeof(rmsg));
		return 0;
	}
	return 0;
}

int handle_client_read(int fd , struct HealerMsg msg)
{
	char objid[OBJIDLENGTH];
	strncpy(objid,msg.content,OBJIDLENGTH);
	HS_DEBUG("get client read msg,objid=%s\n",objid);

	/* first , check whether the obj had been deleted! 
	 * if it has been deleted or don't exist,response COMMAND_OBJ_DELETED
	 * otherwise, get the addr list and offset and objsize
	 */
	uint32_t objsize;
	char sql[SQLLENGTH];
	bzero(sql,SQLLENGTH);
	sprintf(sql,"select * from datatable where objid = '%s'",objid);

	int is_del = 0;
	if(mysql_query(master_conn,sql)){
		is_del = 1;
		HS_DEBUG("maybe the obj has been deleted! error=%s\n",mysql_error(master_conn));
	}else{
		MYSQL_RES *res;
		MYSQL_ROW row;
		res = mysql_use_result(master_conn);
		if((row = mysql_fetch_row(res)) != NULL){
			objsize = atoi(row[1]);
			HS_DEBUG("objid=%s,objsize=%d,del_flag=%d\n",row[0],atoi(row[1]),atoi(row[2]));
			if(atoi(row[2]) == 1){
				is_del = 1;
			}else if(atoi(row[2]) == 0){
				is_del = 0;
			}else{
				HS_ERROR("unexpected del_flag! check it! del_flag=%d\n",atoi(row[2]));
				return -1;
			}
		}else{
			HS_ERROR("mysql_fetch_row error=%s\n",mysql_error(master_conn));
			return -1;
		}
		mysql_free_result(res);
	}

	if(is_del == 1){
		struct HealerMsg msg;
		msg.command = COMMAND_OBJ_DELETED;
		i_send(fd,(void*)&msg,sizeof(struct HealerMsg));
		HS_DEBUG("the objid=%s has been deleted!\n",objid);
		return 0;
	}else{
		/* the obj is exist , then we get the addr list which it stored 
		 * sql table : indextable(objid,ip,read_port,write_port,objoffset 
		 */
		char _sql[SQLLENGTH];
		bzero(_sql,SQLLENGTH);
		sprintf(_sql,"select * from indextable where objid = '%s'",objid);

		if(mysql_query(master_conn,_sql)){
			HS_ERROR("get addr list from indextable failed!\n");
			return -1;
		}else{
			MYSQL_RES *res;
			MYSQL_ROW row;
			res = mysql_store_result(master_conn);
			int row_num;
			if((row_num=mysql_num_rows(res)) == 3){
				struct DataAddrList nodelist;
				struct MetaInfo meta;
				bzero(&nodelist,sizeof(struct DataAddrList));
				bzero(&meta,sizeof(struct MetaInfo));
				int i;
				for(i = 0 ; i < 3 && ((row=mysql_fetch_row(res))!=NULL); i++){
					char *ip = convert_int_to_ip(row[1]);
					printf("ip=%s,read_port=%s,objoffset=%s\n",ip,row[2],row[4]);
					strncpy(nodelist.addr_list[i].address,ip,16);
					nodelist.addr_list[i].read_port = atoi(row[2]);
					meta.offset[i] = atoi(row[4]);
					meta.size[i] = objsize;
				}

				struct HealerMsg smsg;
				bzero(&smsg,sizeof(smsg));
				smsg.command = COMMAND_CLIENT_GET_ADDR_LIST;
				smsg.content_length = sizeof(struct DataAddrList) + sizeof(struct MetaInfo);
				memcpy(smsg.content,&nodelist,sizeof(struct DataAddrList));
				memcpy(smsg.content+sizeof(struct DataAddrList),&meta,sizeof(struct MetaInfo));
				i_send(fd,(void*)&smsg,sizeof(smsg));
			}else{
				HS_ERROR("get addr list row error,expect=3,actual=%d\n",row_num);
				return -1;
			}
			mysql_free_result(res);
		}
	}
	return 0;
}

void on_read(int fd , short ev , void *arg)
{
	LIBEVENT_THREAD *me=arg;
	
	struct HealerMsg msg;
	bzero(&msg,sizeof(msg));
	uint32_t nbytes=i_recv(fd,(void*)&msg,sizeof(msg));
	if(nbytes == sizeof(struct HealerMsg)){
		switch(msg.command){
			case COMMAND_DATANODE_HEARTBEAT:{
				handle_heartbeat(msg);
				break;
			}
			case COMMAND_DATANODE_REGISTER:{
				handle_register(msg);
				break;
			}
			case COMMAND_CLIENT_GET_ADDR_LIST:{
				handle_get_addr_list(fd);
				break;
			}
			case COMMAND_CLIENT_DELETE:{
				handle_delete(fd,msg);
				break;
			}
			case COMMAND_CLIENT_READ:{
				handle_client_read(fd,msg);
				break;
			}
			default:{
				HS_DEBUG("get unknow msg!command=%d\n",msg.command);
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
	if(!client_item){
		printf("calloc client_item failed!\n");
		exit(1);
	}
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

LIBEVENT_THREAD* InitLibevent()
{
	LIBEVENT_THREAD *m_threads=calloc(CORE_COUNT,sizeof(LIBEVENT_THREAD));
	
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
		m_threads[i].new_conn_queue = (struct conn_queue*)malloc(sizeof(struct conn_queue));
		cq_init(m_threads[i].new_conn_queue);
	}
	
	for(i=0 ; i<CORE_COUNT ; i++){
		create_worker(worker_libevent , &m_threads[i]);
	}
	return m_threads;
}

int sql_init()
{
	char *sql_server = "172.16.0.254";
	char *sql_user = "root";
	char *sql_password = "py";
	char *sql_database = "hs";

	master_conn = mysql_init(NULL);
	if(!mysql_real_connect(master_conn,sql_server,sql_user,sql_password,sql_database,0,NULL,0)){
		HS_ERROR("mysql init failed! error:%s\n",mysql_error(master_conn));
		return -1;
	}

	/* 
	 * create database tables 
	　　nodetable(	ip int  not null , read_port smallint not null , 
　　				write_port smallint not null, capacity int not unll,
　　				used_space int not null,free_space int not null,
　　				primary key(ip,read_port,write_port))
　　	datatable:(	objid char(36) not null,objsize int not null,
　　				del_flag tinyint,not null,primary key(objid))
	　　indextable:(objid char(36) not null,ip int not null,
				　　read_port smallint not null,
				　　write_port smallint not null,
				　　objoffset int not null,
				　　primary key(objid,ip,read_port,write_port))
	*/
	char sql_1[SQLLENGTH],sql_2[SQLLENGTH],sql_3[SQLLENGTH];
	bzero(sql_1,SQLLENGTH);
	bzero(sql_2,SQLLENGTH);
	bzero(sql_3,SQLLENGTH);
	sprintf(sql_1,"create table if not exists nodetable(ip int not null , read_port smallint not null ,\
					write_port smallint not null ,capacity int not null , used_space int not null , free_space int not null , primary key(ip,read_port,write_port))");
	sprintf(sql_2,"create table if not exists datatable(objid char(36) not null , objsize int not null , \
					del_flag tinyint not null , primary key(objid))");
	sprintf(sql_3,"create table if not exists indextable(objid char(10) not null , ip int not null , \
					read_port smallint not null , write_port smallint not null, objoffset int not null,\
					primary key(objid,ip,read_port,write_port))");
	if(mysql_query(master_conn,sql_1) || mysql_query(master_conn,sql_2) || mysql_query(master_conn,sql_3)){
		HS_ERROR("create sql table failed!\n");
	}else{
		HS_DEBUG("create sql table sucess!\n");
	}
	return 0;
}

int main(int argc , char **argv)
{
	int i,listen_fd;
	struct sockaddr_in server_addr;
	struct event *server_event;
	int reuseaddr_on =1;
	uint16_t m_port = 30004;
	struct event_base *main_base=NULL;
	LIBEVENT_THREAD *work_threads;

	memset(&m_g_node_time,0,sizeof(m_g_node_time));
	create_worker(check_node_expire,NULL);
	
	conhash = conhash_init(NULL);
	if(conhash){
		HS_INFO("consist hashing init sucess!\n");
	}else{
		HS_ERROR("consist hashing init failed!\n");
		return -1;
	}
	
	if(sql_init() == -1){
		return -1;
	}else{
		HS_DEBUG("sql init sucess!\n");
	}
	
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


