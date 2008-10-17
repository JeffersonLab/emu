OS=`uname`
ARCH=`uname -m`
OSNAME=$OS-$ARCH
echo $OSNAME

#java   -Djava.library.path=$INSTALL_DIR/$OSNAME/lib -DEMU.conf=/CODA/source/emu/conf/EMU2.xml -DKBDControl  -cp ./lib/emu.jar:/CODA/source/xerces-2_9_1/resolver.jar:/CODA/source/xerces-2_9_1/xercesImpl.jar org.jlab.coda.emu.Emu

java   -Djava.library.path=/CODA/source/install/$OSNAME/lib  \
		-DEMU.conf=/CODA/source/emu/conf/EMU2.xml \
		-DKBDControl  \
		-cp ./lib/emu.jar:./lib/modules.jar:/CODA/source/xerces-2_9_1/resolver.jar:/CODA/source/xerces-2_9_1/xercesImpl.jar \
		org.jlab.coda.emu.Emu
