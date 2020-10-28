CC = g++
CFLAGS = -std=c++17 -O3 -g -pthread -Wall 
SOURCE = reader.cc

.PHONY: default all clean reader
default: reader

all: reader

clean:
	-rm ./reader

APACHE_ORC_LIB := build/c++/src/liborc.a
$(APACHE_ORC_LIB):
	cp ../orc/c++/include/orc/orc-config.hh.in ../orc/c++/include/orc/orc-config.hh
	sed -i 's/cmakedefine/define/g' ../orc/c++/include/orc/orc-config.hh
	mkdir -p build
	cd build && cmake ../../orc -DBUILD_JAVA=OFF && make -j4 package

LIBS := $(shell find . -name *.a)
INCLUDES := -I../orc/c++/include -I../orc/c++/src -I../orc/c++/src/sargs -I../orc/c++/src/wrap $(LIBS) -I $(shell pwd)

reader: $(SOURCE) $(APACHE_ORC_LIB)
	$(CC) $(CFLAGS) -o $@ $(SOURCE) $(INCLUDES)
