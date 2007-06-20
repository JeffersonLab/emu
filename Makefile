#
# EMU unix makefile
#   Modification date : $Date$
#   Revision : $Revision$
#   URL : $HeadURL$
 

# operating system, platform, # of processor bits we are using
OSNAME   := $(shell uname)
PLATFORM := $(shell uname -m)
ET_DIR   := $(shell pwd)/../et
LIB_DIR  := $(ET_DIR)/src

# Look to see if the environmental variable ET_USE64BITS is defined.
# If so, then compile everything for 64 bits. Be sure to do a 
# "make cClean" when switching between 32 & 64 bit compiling.
ifeq ($(findstring ET_USE64BITS, $(shell env | grep ET_USE64BITS)), ET_USE64BITS) 
ET_BIT_ARCH = 64
BITS = 64
else
ET_BIT_ARCH =
BITS = 32
endif


MESSAGE = "Make EMU for $(OSNAME) on $(PLATFORM), $(BITS) bits"

# if ARCH is defined, do a vxWorks build
ifeq ($(ARCH),VXWORKSPPC)
MAKEFILE = Makefile.vxworks
OSNAME   = vxworks
MESSAGE  = "Make for $(OSNAME) on $(shell uname), 32 bits"
PLATFORM =
ET_BIT_ARCH = 
endif

# send these definitions down to lower level makefiles
export OSNAME
export PLATFORM
export INC_DIR
export LIB_DIR
export BIN_DIR

# needed directories
DIRS =  $(OSNAME) \
	$(OSNAME)/$(PLATFORM) \
	$(OSNAME)/$(PLATFORM)/$(ET_BIT_ARCH)

# for improved performance declare that none of these targets
# produce files of that name (.PHONY)

.PHONY : all echoarch mkdirs 
.PHONY : clean 

AR          = ar
RANLIB      = ranlib
EMU_DEBUG_FLAG = -DEMU_DEBUG_ON  -DDTD_SUPPORT -DMAX_SPEED -DDTDVALID_SUPPORT
#EMU_DEBUG_FLAG = 
LIBNAM   = libemu.a
SUPLIB = libsupport.a

# for all POSIX systems _REENTRANT makes libc functions reentrant
AC_FLAGS = -D_REENTRANT -D_POSIX_PTHREAD_SEMANTICS

# Linux
ifeq ($(OSNAME),Linux)
CC = gcc
CFLAGS   = -O3 -fPIC -I$(ET_DIR)/src $(AC_FLAGS) $(EMU_DEBUG_FLAG)
LIBS     = -lieee -lrt -lpthread -lm -lnsl -lresolv -ldl
SHLIB_LD = gcc -shared

# if 32 bit
ifneq ($(ET_BIT_ARCH), 64)
CFLAGS   = -m32 -O3 -fpic -I$(ET_DIR)/src  $(AC_FLAGS) $(EMU_DEBUG_FLAG)
SHLIB_LD = gcc -m32 -shared
endif

endif

# Solaris
ifeq ($(OSNAME),SunOS)

CC   = cc
LIBS = -lm -lposix4 -lnsl -lresolv -ldl

# if 64 bit
ifeq ($(ET_BIT_ARCH), 64)

# no static linking on 64 bit solaris
LIB_STATIC =

# if SPARC processor
ifeq ($(PLATFORM), sun4u)
CFLAGS   = -mt -xO5 -xarch=native64 -xcode=pic32 -I$(ET_DIR)/src  -I./emu -I. $(AC_FLAGS) $(EMU_DEBUG_FLAG)
SHLIB_LD = ld -G -L /lib/64
# else if AMD processor
else
# put -fast flag to left of -xarch=amd64 !!!
CFLAGS   = -mt -xO5 -xarch=amd64 -KPIC -I$(ET_DIR)/src  -I. $(AC_FLAGS) $(EMU_DEBUG_FLAG)
# for some reason we must handle ucb lib explicitly
SHLIB_LD = ld -G -L /lib/64 -L /usr/ucblib/amd64
endif

# else if 32 bit
else
CFLAGS   = -mt -xO5 -KPIC -I$(ET_DIR)/src -I. $(EMU_DEBUG_FLAG)
SHLIB_LD = ld -G
endif

endif

# MAC OS
ifeq ($(OSNAME),Darwin)
#Add flags for gdb
#CFLAGS   = -O3 -fPIC -I$(ET_DIR)/src -I. $(AC_FLAGS) $(EMU_DEBUG_FLAG) 
CFLAGS   = -O0 -g3 -fPIC -I$(ET_DIR)/src -I. $(AC_FLAGS) $(EMU_DEBUG_FLAG) 
SHLIB_LD = ld -dylib /usr/lib/dylib1.o -lpthread -ldl -let -Lparsifal -lxmlparse /usr/lib/gcc/darwin/3.3/libgcc.a 
LIBS     = -ldl /usr/lib/gcc/darwin/3.3/libgcc.a 
endif

DEPENDS = dependencies.mk

PROGS =  emu 
#datatype

OBJS = emu_sender.o \
	emu_reader.o \
	emu_process.o 
	
	
SUPOBJS = 	support/gtp.o \
	support/gsh.o \
	support/gsl.o \
	support/gkb.o \
	support/gll.o \
	support/gdf.o \
	support/gparse.o \
	support/gph.o 
	
SRC=emu_emu.c \
    emu_reader.c \
	emu_sender.c \
	emu_reader.c \
	emu_process.c \
	support/gparse.c \
	support/gtp.c \
	support/gsh.c \
	support/gsl.c \
	support/gkb.c \
	support/gll.c \
	support/gdf.c \
	support/gph.c 

PARSIFAL_SRC = parsifal/bistream.c 	parsifal/encoding.c 	parsifal/xmlhash.c 	parsifal/parsifal.c 	parsifal/xmlsbuf.c parsifal/	xmlvect.c 	parsifal/xmlpool.c 	parsifal/dtdvalid.c

PARSIFAL_OBJS =  parsifal/bistream.o parsifal/encoding.o parsifal/xmlhash.o parsifal/parsifal.o parsifal/xmlsbuf.o parsifal/xmlvect.o parsifal/xmlpool.o parsifal/dtdvalid.o

PARSLIB=parsifal/libxmlparse.a

all: echoarch $(PARSLIB) $(SUPLIB) $(LIBNAM)  $(PROGS)

echoarch:
	@echo
	@echo $(MESSAGE)
	@echo
	

mkdirs: mkinstalldirs
	@echo "Creating directories"
	./mkinstalldirs $(DIRS)
	@echo

copyFiles:
	-rm -f *.o *.so *.a $(PROGS)
	-cp -p ./.$(OSNAME)/$(PLATFORM)/$(ET_BIT_ARCH)/* .;

saveFiles:
	-cp -p *.o $(PROGS) $(LIB_USER) ./.$(OSNAME)/$(PLATFORM)/$(ET_BIT_ARCH)/.;
	-rm -f *.o


$(PROGS) : % : %.c $(LIBNAM)  $(SUPLIB) $(PARSLIB) 
	$(CC) -o $@ $(CFLAGS) $< $(LIBNAM) $(SUPLIB) $(PARSLIB) -L$(LIB_DIR) $(LIB_DIR)/libet.a $(LIBS)


$(LIBNAM): $(OBJS)
	$(AR) cr $@ $(OBJS)
	$(RANLIB) $@

$(SUPLIB): $(SUPOBJS)
	$(AR) cr $@ $(SUPOBJS)
	$(RANLIB) $@

$(PARSLIB): $(PARSIFAL_OBJS)
	$(AR) cr $@ $(PARSIFAL_OBJS)
	$(RANLIB) $@

clean: 
	-rm -f core *~ *.o *.so *.a *.P $(PROGS)
	-rm -f support/core support/*~ support/*.o support/*.so support/*.a support/*.P 
	-rm -f parsifal/core parsifal/*~ parsifal/*.o parsifal/*.so parsifal/*.a 
	-rm -f ./.$(OSNAME)/$(PLATFORM)/$(ET_BIT_ARCH)/*
	
MAKEDEPEND = gcc -M $(CFLAGS) -o $*.d $<

#%.o : %.c
#	@echo Compiling - $@ from $<...
#	@$(MAKEDEPEND);  cp $*.d $*.P;  sed -e 's/#.*//' -e 's/^[^:]*: *//' -e 's/ *\\$$//'   -e '/^$$/ d' -e 's/$$/ :/' < $*.d >> $*.P;   rm -f $*.d
#	$(CC) -c $(CFLAGS) -o $@ $<

  %.o : %.c
	@echo Compiling - $@ from $<...
	$(CC) -c $(CFLAGS) -MD -o $@ $<
	@cp $*.d $*.P; \
 sed -e 's/#.*//' -e 's/^[^:]*: *//' -e 's/ *\\$$//' \
  -e '/^$$/ d' -e 's/$$/ :/' < $*.d >> $*.P; \
  rm -f $*.d
            
-include $(SRC:.c=.P)
 
c:
	@echo Compiling $@ from $<...
	$(CC) -o $* $(CFLAGS) $< -L../src -L$(LIB_DIR) $(LIBS)
	
.c.o:
	@echo Compiling - $@ from $<...
	$(CC) -c $(CFLAGS) $<
	
