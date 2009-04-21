package org.jlab.coda.support.codaComponent;

import org.jlab.coda.support.control.State;

/**
 * Created by IntelliJ IDEA.
 * User: heyes
 * Date: Apr 15, 2009
 * Time: 1:54:57 PM
 * To change this template use File | Settings | File Templates.
 */
public interface StatedObject {
    /**
     * The name of the object
     *
     * @return the name
     */
    public String name();

    public State state();
}
