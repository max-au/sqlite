# General variables
C_SRC_DIR := $(shell pwd)
PROJECT_DIR := $(abspath $(C_SRC_DIR)/..)

# Figure out Unix-specific details: library extension
TARGET := $(PROJECT_DIR)/priv/sqlite.so
UNAME_SYS := $(shell uname -s)

ifeq ($(UNAME_SYS), Darwin)
	CC ?= cc
	LDFLAGS ?= -undefined dynamic_lookup
else ifeq ($(UNAME_SYS), FreeBSD)
	CC ?= cc
else ifeq ($(UNAME_SYS), Linux)
	CC ?= gcc
endif

# Erlang NIF includes
ERTS_INCLUDE_DIR = $(shell erl -noshell -eval "io:format(\"~ts/erts-~ts/include/\", [code:root_dir(), erlang:system_info(version)])." -s init stop)

CFLAGS += -std=c99 -finline-functions -Wall -Wmissing-prototypes -fPIC -I $(ERTS_INCLUDE_DIR)

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
    CFLAGS += -DUSE_SYSTEM_SQLITE
    LDLIBS += -lsqlite3
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
