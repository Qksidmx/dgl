CC 			= g++
AR 			= ar rc
RM 			= rm -f
MKDIR		= mkdir -p

CXXFLAGS += -g -std=c++11 -Wall -pthread -DSKG_EDGE_DATA_COLUMN_STOAGE -DSKG_PROPERTIES_SUPPORT_NULL -D_FILE_OFFSET_BITS=64 -DDB_ADAPTER -DSKG_DISABLE_COMPRESSION -DSKG_PREPROCESS_DYNAMIC_EDGE -DUSE_STL_PRIORITY_QUEUE -DSKG_SUPPORT_THREAD_LOCAL -DSKG_QUERY_USE_MT -DSKG_REQ_VAR_PROP -D_SKGNET_STANDALONE_

TOP_DIR=$(shell pwd)/../..
SRC_DIR = $(TOP_DIR)/gfs
OBJ_DIR = $(SRC_DIR)/obj
BIN_DIR = $(TOP_DIR)/bin
INCLUDE_DIRS = -I$(SRC_DIR) -I./

SRCS := $(wildcard *.cc)  # the list of .cc files

LIB_TRGT = libgfs.a
LIB_DEP = libfmt.a

app: $(LG_TRGT) $(NBR_TRGT) $(DGR_TRGT) $(DGR_TRGT) $(VTX_TRGT) $(SPSP_TRGT) $(UPDV_TRGT)
.PHONY: app

####### source sub dirs ##########
SUB_DIRS = 	$(SRC_DIR)/fs \
			$(SRC_DIR)/metrics \
			$(SRC_DIR)/monitoring\
			$(SRC_DIR)/util \
			$(SRC_DIR)/util/threadpool \
			$(SRC_DIR)/env \
			#$(SRC_DIR)/net \
			#$(SRC_DIR)/gq\
			#$(SRC_DIR)/test

sub_dir:
	if [ ! -d $(OBJ_DIR) ]; then \
		-@$(MKDIR) $(OBJ_DIR); \
	fi;
	for SubDir in $(SUB_DIRS); do \
		if [ ! -d $$SubDir ]; then \
			echo "The $$SubDir doesn't exist!"; \
			exit 11;\
		fi;\
		echo "Building $$SubDir ...";\
		make -C $$SubDir --no-print-directory all;\
	done
.PHONY: sub_dir


all: $(LIB_TRGT) app
.PHONY: all

$(OBJ_DIR)/%.o : %.cc 
	$(CC) $(CXXFLAGS) $(INCLUDE_DIRS) -c $< -o $@

%: %.cc $(LIB_TRGT)
	$(CC) $(CXXFLAGS) $(INCLUDE_DIRS) -o $@ $< $(LIB_TRGT) $(LIB_DEP)

lib: sub_dir 
	$(AR) $(LIB_TRGT) $(shell find $(OBJ_DIR) -type f \( -iname "*.o" ! -iname "lg.o" ! -iname "nbr.o" \))
