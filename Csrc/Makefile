#
# emu top level Makefile


MAKEFILE = Makefile

# if using vxworks, use different set of the lowest level makefiles
ifeq ($(BMS_OS), vxworks)
  ifdef BMS_ARCH
    MAKEFILE = Makefile.$(BMS_OS)-$(BMS_ARCH)
  else
    $(error "Need to define BMS_ARCH if using OS = vxworks")
  endif
endif

# define TOPLEVEL for use in making doxygen docs
TOPLEVEL = .

# list directories in which there are makefiles to be run (relative to this one)
SRC_DIRS = libsrc execsrc

# declaring a target phony skips the implicit rule search and saves time
.PHONY : first help doc tar

OPTIMIZE = 3
#DEBUG = false

first: all

help:
	@echo "make [option]"
	@echo "      env       - list env variables"
	@echo "      mkdirs    - make necessary directories for C,C++"
	@echo "      install   - install all headers and compiled files for C,C++"
	@echo "      uninstall - uninstall all headers and compiled files for C,C++"
	@echo "      relink    - delete libs and executables, and relink object files"
	@echo "      clean     - delete all exec, library, object, and dependency files"
	@echo "      distClean - clean and remove hidden OS directory"
	@echo "      execClean - delete all exec and library files"


# Use this pattern rule for all other targets
%:
	@for i in $(SRC_DIRS); do \
	   $(MAKE) -C $$i -f $(MAKEFILE) $@; \
	done;
