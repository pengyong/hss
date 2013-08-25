#ifndef __HS_DEF_H__
#define __HS_DEF_H__

#define COMMAND_DATANODE_REGISTER		1
#define COMMAND_DATANODE_HEARTBEAT		2
#define COMMAND_DATANODE_READ			3
#define COMMAND_DATANODE_WRITE			4
#define COMMAND_DATANODE_REMOTE_WRITE	5
#define COMMAND_CLIENT_GET_ADDR_LIST	6
#define COMMAND_CLIENT_DELETE			7
#define COMMAND_CLIENT_READ				8
#define COMMAND_OBJ_DELETED				9
#define COMMAND_DATANODE_READ_FAILED	10

#define DATALENGTH						1024
#define OBJIDLENGTH						36
#define SQLLENGTH						300
#define NODE_MAX_NUM 					100
#define SIZE_PER_V_NODE					(1<<20)
#define BACKLOG 						10

/* backend libevent ,each libevent thread in one core */
#define CORE_COUNT						8

#define USING_DEBUG
#ifdef USING_DEBUG
#define HS_INFO(fmt , args...) printf("[%s]:%d:"fmt, __FUNCTION__,__LINE__,##args)
#define HS_DEBUG(fmt , args...) printf("[%s]:%d:"fmt, __FUNCTION__,__LINE__,##args)
#define HS_ERROR(fmt , args...) printf("[%s]:%d:"fmt, __FUNCTION__,__LINE__,##args)

#else
#define HS_INFO(fmt , args...) printf("[%s]:%d:"fmt, __FUNCTION__,__LINE__,##args)
#define HS_DEBUG(fmt , args...) printf("")
#define HS_ERROR(fmt , args...) printf("[%s]:%d:"fmt, __FUNCTION__,__LINE__,##args)
#endif

#endif

