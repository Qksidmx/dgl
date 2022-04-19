CC = g++
AR = ar rc
RM = rm
#TOP_DIR= /home/skg/work/gfs
TOP_DIR=$(shell pwd)/../..
SRC_DIR = $(TOP_DIR)/rev
OBJ_DIR = $(TOP_DIR)/rev/obj
INCLUDE_DIRS = -I$(SRC_DIR)
SRCS := $(wildcard *.cc)
HDRS := $(wildcard *.h)
OBJS := $(patsubst %.cc, $(OBJ_DIR)/%.o, $(SRCS))
DEPS := $(OBJS:.o=.d)

CXXFLAGS += -g -std=c++11 -Wall -DSKG_EDGE_DATA_COLUMN_STOAGE -DSKG_PROPERTIES_SUPPORT_NULL -D_FILE_OFFSET_BITS=64 -DDB_ADAPTER -DSKG_DISABLE_COMPRESSION -DSKG_PREPROCESS_DYNAMIC_EDGE -DUSE_STL_PRIORITY_QUEUE -DSKG_SUPPORT_THREAD_LOCAL -DSKG_QUERY_USE_MT -DSKG_REQ_VAR_PROP


all: $(DEPS) $(OBJS)
.PHONY:all

$(OBJ_DIR)/%.o : %.cc 
	$(CC) $(CXXFLAGS) $(INCLUDE_DIRS) -c $< -o $@

$(OBJ_DIR)/%.d : %.cc
	@set -e; \
	$(CC) -MM $(CXXFLAGS) $(INCLUDE_DIRS) $< > $@.$$$$; \
	sed 's,\($*\)\.o[ :]*,\$@ : ,g' < $@.$$$$ > $@.$$$$t; \
	sed 's,\.d,\.o ,g' < $@.$$$$t > $@; \
	rm -f $@.$$$$ $@.$$$$t

# sed 's,\($*\)\.o[ :]*,\1.o $@ : ,g' < $@.$$$$ > $@.$$$$t; Here has two targets, will not use

-include $(DEPS)

clean:
	rm -f $(DEPS) $(OBJS)
