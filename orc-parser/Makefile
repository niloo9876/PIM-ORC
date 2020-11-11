CC = g++
CFLAGS = -std=c++17 -O3 -g -pthread -Wall 
SOURCE = reader.cc

.PHONY: default all clean reader
default: reader

all: reader

clean:
	rm ./reader
	rm -rf build/

SNAPPY_HOME := $(shell cd ../snappy; pwd)
SNAPPY_LIB := ../build/snappy/libsnappy.a
$(SNAPPY_LIB):
	mkdir -p build
	cd build && mkdir -p snappy
	cd build/snappy && cmake ../../../snappy && make
	cp build/snappy/snappy-stubs-public.h ../snappy
	cp build/snappy/libsnappy.a ../snappy

APACHE_ORC_LIB := build/c++/src/liborc.a
$(APACHE_ORC_LIB):
	cp ../orc/c++/include/orc/orc-config.hh.in ../orc/c++/include/orc/orc-config.hh
	sed -i 's/cmakedefine/define/g' ../orc/c++/include/orc/orc-config.hh
	mkdir -p build
	export SNAPPY_HOME=$(SNAPPY_HOME)
	cd build && cmake -DBUILD_JAVA=OFF ../../orc && make -j4 package

INCLUDES := -I../orc/c++/include -I../orc/c++/src -I../orc/c++/src/sargs -I../orc/c++/src/wrap -I $(shell pwd)
LIBS = $(shell find . -name *.a)

reader: $(SOURCE) $(SNAPPY_LIB) $(APACHE_ORC_LIB)
	$(CC) $(CFLAGS) -o $@ $(SOURCE) $(INCLUDES) $(LIBS)
