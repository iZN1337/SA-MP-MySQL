CCPP=clang++ -m32
CC=clang -m32


COMPILE_FLAGS=-c -O3 -w -fPIC -DLINUX -Wall -Isrc/SDK/amx/ -Isrc/ -ferror-limit=1


all: compile dynamic_link static_link clean
dynamic: compile dynamic_link clean
static: compile static_link clean

compile:
	@mkdir -p bin
	@echo Compiling plugin..
	@ $(CCPP) $(COMPILE_FLAGS) -std=c++11 src/*.cpp
	@echo Compiling plugin SDK..
	@ $(CCPP) $(COMPILE_FLAGS) src/SDK/*.cpp
	@ $(CC) $(COMPILE_FLAGS) src/SDK/amx/*.c

dynamic_link:
	@echo Linking \(dynamic\)..
	@ $(CCPP) -O3 -fshort-wchar -shared -o "bin/mysql.so" *.o -lmysqlclient_r -pthread -lrt

static_link:
	@echo Linking \(static\)..
	@ $(CCPP) -O3 -fshort-wchar -shared -o "bin/mysql_static.so" *.o ./src/mysql_lib/libmysqlclient_r.a -pthread -lrt

clean:
	@ rm -f *.o
	@echo Done.
