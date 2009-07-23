
ifndef BMS_HOME
  $(error "Need to define BMS_HOME")
endif

ifeq ($(BMS_OSNAME), vxworks)
  $(error "EMU is for unix only")
endif

# Setting TOPLEVEL tells Makefile.common where to find Makefile.local
# and to include it if make is run from this dir or the top-level package
# directory.
TOPLEVEL = ../..

# use cMsg libraries

# only here for print out
MODULE_NAME = emu
PACKAGES = et cMsg emu

#DEBUG = true
CODA_USE64BITS=true
OPTIMIZE = 3

CFLAGS   += -I../libsrc -rdynamic
CXXFLAGS += -I../libsrc -rdynamic

include $(BMS_HOME)/Makefile.common

include $(BMS_HOME)/Makefile.bin

# Setting LIB_SRCDIR tells make where to find the libraries needed by the executable
# Setting BIN_DEPENDS tels make which libraries need to be up-to-date to build the executable

LIB_SRCDIR = $(TOPLEVEL)/Csrc/libsrc

# add the libraries to the build
ifeq ($(BMS_OS),Darwin)
MISC_LIBS += $(BIN_DEPENDS) -framework JavaVM -framework CoreFoundation
else
#MISC_LIBS += -rdynamic -Wl,--whole-archive -Wl,--no-whole-archive -ldl -Wl,-E
MISC_LIBS += -rdynamic -Wl,-E
endif

# tell make that the executable depends on the libraries

emu$(DEBUG_SUFFIX): emu.cc
transport_test$(DEBUG_SUFFIX): transport_test.cc transport_test.h
