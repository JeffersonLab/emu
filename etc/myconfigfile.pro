-injars       build/lib/emu-3.1.jar
-outjars      build/lib/emu-3.1.optimized.jar
-libraryjars  <java.home>/lib/rt.jar
-libraryjars  java/jars/jevio-5.2.jar
-libraryjars  java/jars/disruptor-3.4.3.jar
-libraryjars  java/jars/cMsg-6.0.jar
-libraryjars  java/jars/et-16.2.jar
-libraryjars  java/jars/swing-layout.jar

-optimizationpasses 3
-overloadaggressively
-allowaccessmodification
-verbose

-keep public class org.jlab.coda.emu.EmuFactory {
    public static void main(java.lang.String[]);
}

-keep public class * {
    public protected *;
}

-keepclasseswithmembernames,includedescriptorclasses class * {
    native <methods>;
}

-keepclassmembers,allowoptimization enum * {
    public static **[] values();
    public static ** valueOf(java.lang.String);
}

