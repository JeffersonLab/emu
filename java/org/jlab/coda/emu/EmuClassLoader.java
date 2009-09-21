/*
 * Copyright (c) 2008, Jefferson Science Associates
 *
 * Thomas Jefferson National Accelerator Facility
 * Data Acquisition Group
 *
 * 12000, Jefferson Ave, Newport News, VA 23606
 * Phone : (757)-269-7100
 *
 */

package org.jlab.coda.emu;

import java.net.URLClassLoader;
import java.net.URL;
import java.lang.reflect.Field;

/**
 * This class is a custom classLoader which delegates all loading to its parent (in this case, system)
 * classLoader except for a single named class which is "refound" and reloaded using standard URLClassLoader
 * means.<p>
 *
 * This class is used to load module and transport classes at "download" in order to create the necessary
 * (from configuration) module and transport objects. The manner in which this class is used is as follows.
 * Either a public interface (myInterface) or (public?) superclass (mySuperclass) is defined such that:<p>
 *<pre><code>
 *           public interface myInterface
 *           class myClass implements myInterface    or
 *           class mySuperclass extends myClass
 * </code></pre>
 *
 * Each time myClass is loaded, a new object of this type is used:<p>
 *<pre><code>
 *           EmuClassLoader emuClassloader = new EmuClassLoader();
 *           emuClassloader.setClassToLoad("myClass");
 *           Class myObjectClass  = emuClassloader.loadClass("myClass");
 *           myInterface  object1 = (myInterface)  myObjectClass.newInstance();
 *           mySuperClass object2 = (mySuperClass) myObjectClass.newInstance();
 *
 *           // create new class loader so classes can be reloaded
 *           emuClassloader = new EmuClassLoader();
 *           emuClassloader.setClassToLoad("myClass");
 *           myObjectClass  = emuClassloader.loadClass("myClass");
 *           object1 = (myInterface)  myObjectClass.newInstance();
 *           object2 = (mySuperClass) myObjectClass.newInstance();
 * </code></pre>
 * The reloading of that single class (myClass) in ensured if and only if:
 * <ol>
 * <li>a new object of this type (EmuClassLoader) is used each time myClass needs reloading
 * <li>the setClassToLoad("myClass") method is called, else this class defaults
 * to standard URLClassLoader behavior (having parent classloader load everything it can)
 * </ol>
 * Using this object more than once to load the same class will result in the same class
 * returned both times,
 *
 * <b>Dynamic Class Reloading</b><p>
 * (from http://tutorials.jenkov.com/java-reflection/dynamic-class-loading-reloading.html)<p>
 *
 * Dynamic class reloading is a bit more challenging. Java's builtin Class loaders always check if a class is already
 * loaded before loading it. Reloading the class is therefore not possible using Java's builtin class loaders.
 * To reload a class you will have to implement your own ClassLoader subclass.<p>
 *
 * Even with a custom subclass of ClassLoader you have a challenge. Every loaded class needs to be linked.
 * This is done using the ClassLoader.resolve() method. This method is final, and thus cannot be overridden in your
 * ClassLoader subclass. The resolve() method will not allow any given ClassLoader instance to link the same class
 * twice. Therefore, everytime you want to reload a class you must use a new instance of your ClassLoader subclass.
 * This is not impossible, but necessary to know when designing for class reloading.<p>
 *
 * The trick is to:
 * <ol>
 * <li>  Use an interface as the variable type, and just reload the implementing class, or
 * <li>  Use a superclass as the variable type, and just reload a subclass.
 * </ol><p>
 * Either of these two methods will work if the type of the variable, the interface or superclass,
 * is not reloaded when the implementing class or subclass is reloaded.<p>
 *
 * To make this work you will of course need to implement your class loader to let the interface or superclass be
 * loaded by its parent. When your class loader is asked to load the myClass class, it will also be asked to load
 * the myInterface class, or the mySuperclass class, since these are referenced from within the myClass
 * class. Your class loader must delegate the loading of those classes to the same class loader that loaded the class
 * containing the interface or superclass typed variables.<p>
 *
 * <b>Class Unloading</b><p>
 * To unload a class you have to create a custom classloader and load the class using it.
 * After you are done with the class you need to release all references to the class as
 * well as to the class loader by reassigning the variables or setting them to null.
 * Then either wait for System.gc() to unload the class or you call it directly in a loop
 * till no more bytes can be freed. However normally calling it twice does the trick, but
 * that did NOT work for me (timmer) in the emu code.
 *
 */
public class EmuClassLoader extends URLClassLoader {

    /** Name of the classes we want loaded. */
    String[] classesToLoad;

    /**
     * Constructor. The system class loader is assumed to be the parent.
     *
     * @param urls URLs to search for the classes of interest.
     */
    public EmuClassLoader(URL[] urls) {
        super(urls);
    }

    /**
     * Constructor.
     *
     * @param urls   URLs to search for the classes of interest.
     * @param parent parent classloader
     */
    public EmuClassLoader(URL[] urls, ClassLoader parent) {
        super(urls, parent);
    }

    /**
     * Set the name of the classes we want to load.
     *
     * @param classNames names of the classes we want to load
     */
    public void setClassesToLoad(String[] classNames) {
        classesToLoad = classNames;
    }

    /**
     * Method to load the class of interest. This is called many
     * times by the system - once for each class that the class
     * we are trying to load references.
     *
     * @param name name of class to load
     * @return Class object
     * @throws ClassNotFoundException if code for class cannot be found
     */
    public Class loadClass(String name) throws ClassNotFoundException {

        // Default to standard URLClassLoader behavior
        if (classesToLoad == null) return getParent().loadClass(name);

        // We only want to load 1 specific class and all of its inner classes.
        // Everything else needs to be loaded by the parent classloader
        // (the system loader in this case).
        // We especially do not want to load "org.jlab.coda.emu.EmuModule"
        // since that will not be the same class as the identically named
        // one loaded by the system loader. Because all modules implement
        // the EmuModule interface, this loader will be asked to load it.
        // Simply pass the request back to the parent and we're OK. Now
        // there's only one version of EmuModule that everyone agrees on.
        //
        // Inner classes have the same name as the enclosing class with
        // an additional string on the end so simply scan to see if the
        // class name CONTAINS the name of the original class.
        boolean matches = false;
        for (String clazz : classesToLoad) {
            if (name.contains(clazz)) {
                matches = true;
                break;
            }
        }
        
        if (!matches) {
//System.out.println("$$$$ PPPP EmuClassLoader: have parent load " + name);
            return getParent().loadClass(name);
        }

        // If this loader has already loaded the class, return it since it
        // is not allowed to load things twice.
        Class c = findLoadedClass(name);
        if (c != null) {
//System.out.println("$$$$ XXXX EmuClassLoader: returning already loaded class " + name);
            return c;
        }

        // Cannot call super.loadClass(name) since that calls ClassLoader.loadClass(name)
        // (since URLClassLoader never overwrites it), which in turn delegates the finding
        // to the parent. We want to use the URLClassLoader to find it from scratch so call
        // the findClass() method directly.


//System.out.println("$$$$ !!!! EmuClassLoader: loading " + name);
        c = findClass(name);
        return c;
    }

}
