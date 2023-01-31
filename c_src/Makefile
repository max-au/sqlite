# General variables
C_SRC_DIR := $(shell pwd)
PROJECT_DIR := $(abspath $(C_SRC_DIR)/..)

# Figure out OS-specific details: library extension
ifeq ($(OS),Windows_NT)
	TARGET := $(PROJECT_DIR)/priv/sqlite.dll
else
	TARGET := $(PROJECT_DIR)/priv/sqlite.so
	UNAME_SYS := $(shell uname -s)

	ifeq ($(UNAME_SYS), Darwin)
		CC ?= cc
		LDFLAGS ?= -flat_namespace -undefined suppress
	else ifeq ($(UNAME_SYS), FreeBSD)
		CC ?= cc
	else ifeq ($(UNAME_SYS), Linux)
		CC ?= gcc
	endif
endif

# Figure out Erlang Library paths.
# Do a single call to "erl" printing all the information, and split it in words later.
# This saves quite an amount of time spawning extra BEAMs
DIRS = $(shell erl -noshell -eval "io:format(\"~ts/erts-~ts/include/~n~ts~n~ts\", [code:root_dir(), erlang:system_info(version), code:lib_dir(erl_interface, include), code:lib_dir(erl_interface, lib)])." -s init stop)

ERTS_INCLUDE_DIR ?= $(word 1, $(DIRS))
ERL_INTERFACE_INCLUDE_DIR ?= $(word 2, $(DIRS))
ERL_INTERFACE_LIB_DIR ?= $(word 3, $(DIRS))

CFLAGS += -std=c99 -finline-functions -Wall -Wmissing-prototypes -fPIC -I $(ERTS_INCLUDE_DIR) -I $(ERL_INTERFACE_INCLUDE_DIR)
LDLIBS += -L $(ERL_INTERFACE_LIB_DIR) -lei -lsqlite3

# Set up DEBUG flags if DEBUG environment variable is set
# The "variable trick" below allows using "DEBUG=1"
ifdef DEBUG
    CFLAGS += -O0 -g3 -fno-omit-frame-pointer -DSQLITE_DEBUG
else
    CFLAGS += -O3
endif

# Always build a shared binary (so, dll)
LDFLAGS += -shared

# Sources and object files in C_SRC
#SRC := $(shell find $(C_SRC_DIR) -type f \( -name "*.c" \))
ifdef USE_SYSTEM_SQLITE
    SRC := ${C_SRC_DIR}/sqlite_nif.c
    CFLAGS += -DUSE_SYSTEM_SQLITE -DNO_SQLITE3_ERROR_OFFSET
else
	# SQLite amalgamation is a part of this project, but it's possible
	#  that someone wants to use the system-provided one
	SRC := ${C_SRC_DIR}/sqlite_nif.c ${C_SRC_DIR}/sqlite3.c
endif

OBJ = $(addsuffix .o, $(basename $(SRC)))

# Disable implicit rules, they aren't helpful for this Makefile, polluting debug output
.SUFFIXES:

$(TARGET): $(OBJ)
	$(CC) $(OBJ) $(LDFLAGS) $(LDLIBS) -o $(TARGET)

%.o: %.c
	$(CC) $(CFLAGS) -o $@ -c $<

.PHONY: clean

clean:
	@rm -f $(OBJ) $(TARGET)
