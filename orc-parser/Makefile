CC = g++
CFLAGS = -std=c++17 -O3 -g -pthread -Wall 
DPU_OPTS = `dpu-pkg-config --cflags --libs dpu` 
SOURCE = reader.cc

.PHONY: default all clean reader
default: reader

all: reader

clean:
	rm ./reader
	rm -rf build/

NR_DPUS = 1
NR_TASKLETS = 1
USE_PIM = OFF

SNAPPY_HOME := $(shell cd ../snappy; pwd)
DPU_PROGRAM := \"$(SNAPPY_HOME)/pim-snappy/decompress.dpu\"

SNAPPY_LIB := ../build/snappy/libsnappy.a
$(SNAPPY_LIB):
	cd $(SNAPPY_HOME)/pim-snappy && make NR_DPUS=$(NR_DPUS) NR_TASKLETS=$(NR_TASKLETS)
	mkdir -p build
	cd build && mkdir -p snappy
	cd build/snappy && cmake -DUSE_PIM=$(USE_PIM) -DNR_DPUS=$(NR_DPUS) -DNR_TASKLETS=$(NR_TASKLETS) -DDPU_PROGRAM=$(DPU_PROGRAM) ../../../snappy && make 
	cp build/snappy/snappy-stubs-public.h ../snappy
	cp build/snappy/libsnappy.a ../snappy

APACHE_ORC_LIB := build/c++/src/liborc.a
$(APACHE_ORC_LIB):
	cp ../orc/c++/include/orc/orc-config.hh.in ../orc/c++/include/orc/orc-config.hh
	sed -i 's/cmakedefine/define/g' ../orc/c++/include/orc/orc-config.hh
	mkdir -p build
	cd build && export SNAPPY_HOME=$(SNAPPY_HOME) && cmake -DBUILD_JAVA=OFF ../../orc && make -j4 package

INCLUDES := -I../orc/c++/include -I../orc/c++/src -I../orc/c++/src/sargs -I../orc/c++/src/wrap -I $(shell pwd)
LIBS = $(shell find . -name *.a)

reader: $(SOURCE) $(SNAPPY_LIB) $(APACHE_ORC_LIB)
	$(CC) $(CFLAGS) -o $@ $(SOURCE) $(INCLUDES) $(LIBS) $(DPU_OPTS)
