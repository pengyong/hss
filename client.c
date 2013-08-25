/*****************************************************************************
* Copyright		        : 
* Version		        : 0.1
* File Name		        : client.c
* Description	        : 
* Author                : pengyong
* Creation              : 2013-4-24
* Modification	        : 2013-5-12
******************************************************************************/

#include "stdio.h"
#include "global.h"
#include "def.h"

int server_fd;
void print_help(){
	printf("usage:./client -O objid -M mode -F path -h\n");
	printf("	-O objid	: the data named objid to read,write,delete from server\n");
	printf("	-M mode		: r:read , w:write , d:delete\n");
	printf("	-F path		: if read,it is the data store path,if write,it is the data path\n");
	printf("	-h		: help\n");

}

int get_addr_list(struct DataAddrList *addr_list)
{
	struct HealerMsg smsg,rmsg;
	smsg.command = COMMAND_CLIENT_GET_ADDR_LIST;
	i_send(server_fd,(void*)&smsg,sizeof(smsg));
	bzero(&rmsg,sizeof(rmsg));
	i_recv(server_fd,(void*)&rmsg,sizeof(rmsg));
	if(rmsg.command == COMMAND_CLIENT_GET_ADDR_LIST){
		memcpy(addr_list,rmsg.content,sizeof(struct DataAddrList));
	}else{
		HS_ERROR("get response msg failed!command=%d\n",rmsg.command);
		return -1;
	}
	HS_DEBUG("addr list:\n");
	HS_DEBUG("[0]:ip=%s , read_port=%d , write_port=%d\n",addr_list->addr_list[0].address,addr_list->addr_list[0].read_port,addr_list->addr_list[0].write_port);
	HS_DEBUG("[1]:ip=%s , read_port=%d , write_port=%d\n",addr_list->addr_list[1].address,addr_list->addr_list[1].read_port,addr_list->addr_list[1].write_port);
	HS_DEBUG("[2]:ip=%s , read_port=%d , write_port=%d\n",addr_list->addr_list[2].address,addr_list->addr_list[2].read_port,addr_list->addr_list[2].write_port);
	return 0;
}

void handle_read(char *objid , char *path)
{
	HS_DEBUG("objid=%s,path=%s\n",objid,path);
	struct HealerMsg msg;
	bzero(&msg,sizeof(msg));
	msg.command = COMMAND_CLIENT_READ;
	strncpy(msg.content,objid,OBJIDLENGTH);
	i_send(server_fd,(void*)&msg,sizeof(struct HealerMsg));

	/* reveive response , include addr list and meta info(size,offset)
	 * if sucess,the response command is COMMAND_CLIENT_GET_ADDR_LIST,
	 * which content contain sizeof(struct DataAddrList) + sizeof(struct MetaInfo)
	 * if failed,the response command is COMMAND_OBJ_DELETED
	 */
	struct DataAddrList addr_list;
	struct MetaInfo meta;
	struct HealerMsg _msg;
	bzero(&_msg,sizeof(struct HealerMsg));
	i_recv(server_fd,(void*)&_msg,sizeof(struct HealerMsg));
	if(_msg.command == COMMAND_OBJ_DELETED){
		HS_DEBUG("Sorry,the objid=%s has beed deleted!\n",objid);
		return;
	}else if(_msg.command == COMMAND_CLIENT_GET_ADDR_LIST){
		memcpy(&addr_list,_msg.content,sizeof(struct DataAddrList));
		memcpy(&meta,_msg.content+sizeof(struct DataAddrList),sizeof(struct MetaInfo));
		HS_DEBUG("get the meta info from server sucess!\n");
	}
	
	int datanode_fd;
	struct sockaddr_in datanode_addr;
	int trys = 0;
	while(trys < 3){
		datanode_fd = i_socket(AF_INET,SOCK_STREAM,0);
		bzero(&datanode_addr,sizeof(datanode_addr));
		datanode_addr.sin_family=AF_INET;
		datanode_addr.sin_port = htons(addr_list.addr_list[trys].read_port);
		datanode_addr.sin_addr.s_addr = inet_addr(addr_list.addr_list[trys].address);
		i_connect(datanode_fd,(struct sockaddr*)&datanode_addr,sizeof(datanode_addr));

		struct DataMsg sendmsg,readmsg;
		bzero(&sendmsg,sizeof(sendmsg));
		bzero(&readmsg,sizeof(readmsg));
		sendmsg.command = COMMAND_DATANODE_READ;
		meta.index = trys;
		memcpy(sendmsg.data,&meta,sizeof(meta));
		i_send(datanode_fd,(void*)&sendmsg,sizeof(sendmsg));
		i_recv(datanode_fd,(void*)&readmsg,sizeof(readmsg));
		if(readmsg.command == COMMAND_DATANODE_READ_FAILED){
			trys++;
			HS_DEBUG("read failed,try next datanode\n");
			continue;	
		}else if(readmsg.command == COMMAND_DATANODE_READ){
			/* read data sucess,now we should store the data in specify dir*/
			HS_DEBUG("read data sucess!try times=%d\n",trys);
			int filefd = open(path,O_WRONLY | O_CREAT);
			int nbytes;
			if((nbytes = write(filefd,readmsg.data,readmsg.objsize)) == -1){
				HS_ERROR("write data to file failed!\n");
			}
			close(filefd);
			return;
		}else{
			HS_ERROR("unknow msg!command=%d\n",readmsg.command);
			return ;
		}
	}
}

void handle_write(char *objid , char *path)
{
	HS_DEBUG("objid=%s,path=%s\n",objid,path);
	struct DataAddrList addr_list;
	if(0 == get_addr_list(&addr_list)){
		/* connect to the first datanode and send data*/
		int first_datanode_fd;
		struct sockaddr_in datanode_addr;
		first_datanode_fd = i_socket(AF_INET,SOCK_STREAM,0);
		bzero(&datanode_addr,sizeof(datanode_addr));
		datanode_addr.sin_family=AF_INET;
		datanode_addr.sin_port = htons(addr_list.addr_list[0].write_port);
		datanode_addr.sin_addr.s_addr = inet_addr(addr_list.addr_list[0].address);
		i_connect(first_datanode_fd,(struct sockaddr*)&datanode_addr,sizeof(datanode_addr));

		/* read data from path */
		int filefd = open(path,O_RDONLY);
		if(filefd == -1){
			HS_ERROR("open %s failed!\n",path);
			exit(1);
		}
		struct stat filestat;
		if(stat(path,&filestat) == -1){
			HS_ERROR("stat failed!\b");
			exit(1);
		}
		uint32_t objsize = (uint32_t)filestat.st_size;
		if(objsize > DATALENGTH){
			HS_ERROR("Notice:the data you want to write is too large!\n");
		}
		char *databuf = (char*)malloc(objsize*sizeof(char));
		int nbytes;
		if((nbytes = read(filefd,databuf,objsize)) == -1){
			HS_ERROR("read data failed!path=%s\n",path);
			exit(1);
		}
		
		struct DataMsg msg;
		msg.command = COMMAND_DATANODE_WRITE;
		msg.data_list = addr_list;
		strncpy(msg.objid,objid,OBJIDLENGTH);
		strncpy(msg.data,databuf,objsize);
		msg.objsize = objsize;
		i_send(first_datanode_fd,(void*)&msg,sizeof(msg));
		/* reveive the write response */
		struct ResponseMsg rmsg;
		i_recv(first_datanode_fd,(void*)&rmsg,sizeof(rmsg));
		if(rmsg.command == COMMAND_DATANODE_WRITE && rmsg.flag == 0){
			HS_DEBUG("write sucess!\n");
		}else{
			HS_ERROR("write failed!objid=%s,command=%d,flag=%d\n",objid,rmsg.command,rmsg.flag);
		}
	}else{
		HS_ERROR("get addr list failed!\n");
	}
}

void handle_delete(char* objid)
{
	HS_DEBUG("objid=%s\n",objid);
	struct HealerMsg msg;
	msg.command = COMMAND_CLIENT_DELETE;
	strncpy(msg.content,objid,OBJIDLENGTH);
	i_send(server_fd,(void*)&msg,sizeof(msg));

	struct ResponseMsg rmsg;
	bzero(&rmsg,sizeof(rmsg));
	i_recv(server_fd,(void*)&rmsg,sizeof(rmsg));
	if(rmsg.command == COMMAND_CLIENT_DELETE && rmsg.flag == 0){
		HS_DEBUG("delete sucess!\n");
	}else{
		HS_ERROR("delete failed!objid=%s,command=%d,flag=%d\n",objid,rmsg.command,rmsg.flag);
	}

}

int main(int argc , char **argv)
{
	//int server_fd;
	uint16_t server_port = 30004;
	struct sockaddr_in server_addr;
	char optchar;
	char objid[OBJIDLENGTH];
	char path[50];
	char mode[2];
	while((optchar = getopt(argc ,argv , "hO:M:F:"))!= -1){
		switch(optchar){
			case 'O':{
				bzero(objid,sizeof(objid));
				strcpy(objid,optarg);
				break;
			}
			case 'M':{
				bzero(mode,sizeof(mode));
				strcpy(mode,optarg);
				break;
			}
			case 'F':{
				bzero(path,sizeof(path));
				strcpy(path,optarg);
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

	server_fd = i_socket(AF_INET,SOCK_STREAM,0);
	bzero(&server_addr,sizeof(server_addr));
	server_addr.sin_family=AF_INET;
	server_addr.sin_port=htons(server_port);
	server_addr.sin_addr.s_addr=INADDR_ANY;
	i_connect(server_fd,(struct sockaddr*)&server_addr,sizeof(server_addr));
	
	switch(mode[0]){
		case 'r':{
			handle_read(objid,path);
			break;
		}
		case 'w':{
			handle_write(objid,path);
			break;
		}
		case 'd':{
			handle_delete(objid);
			break;
		}
		default:{
			HS_ERROR("error:check your input!\n");
			exit(1);
		}
	}
		
	return 0;
}


