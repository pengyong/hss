
all:node master client clear
.PHONY:all

MYSQLLIBS=/usr/lib64/mysql
#MYSQLLIBS=/usr/local/lib/mysql
LCONHASHLIBS=ConHash/bin

node:datanode.c def.h global.h
	gcc -o hsnode datanode.c -levent -lpthread -lmysqlclient -L$(MYSQLLIBS)

master:master.c def.h global.h
	gcc -o hsmaster master.c -levent -lpthread -lmysqlclient -L$(MYSQLLIBS) -lconhash -L$(LCONHASHLIBS)

client:client.c def.h global.h
	gcc -o hsclient client.c -levent -lpthread

clear:clear.c
	gcc -o hsclear clear.c -lmysqlclient -L$(MYSQLLIBS)

clean:
	rm -f hsnode hsmaster hsclient hs clear

