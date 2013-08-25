
#include <stdio.h>
#include <string.h>
#include "def.h"
#include "mysql/mysql.h"

int main(int argc , char **argv)
{
	char *sql_server = "172.16.0.254";
	char *sql_user = "root";
	char *sql_password = "py";
	char *sql_database = "hs";
	MYSQL *conn;
	conn = mysql_init(NULL);
	if(!mysql_real_connect(conn,sql_server,sql_user,sql_password,sql_database,0,NULL,0)){
		HS_ERROR("mysql init failed! error:%s\n",mysql_error(conn));
		return -1;
	}

	char sql_1[SQLLENGTH],sql_2[SQLLENGTH],sql_3[SQLLENGTH];
	bzero(sql_1,SQLLENGTH);
	bzero(sql_2,SQLLENGTH);
	bzero(sql_3,SQLLENGTH);
	sprintf(sql_1,"drop table indextable;");
	sprintf(sql_2,"drop table nodetable;");
	sprintf(sql_3,"drop table datatable;");

	if(mysql_query(conn,sql_1) == 0 && mysql_query(conn,sql_2) == 0 && mysql_query(conn,sql_3) == 0){
		printf("clear ok!\n");
	}else{
		printf("clear failed!error=%s\n",mysql_error(conn));
	}
	system("rm -f data1 data2 data3 data4 hii");
	return 0;
	
}
