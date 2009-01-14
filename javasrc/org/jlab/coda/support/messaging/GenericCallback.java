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

package org.jlab.coda.support.messaging;

/**
 * Created by IntelliJ IDEA.
 * User: heyes
 * Date: Sep 24, 2008
 * Time: 8:48:56 AM
 * To change this template use File | Settings | File Templates.
 */
class GenericCallback {
    /**
     * Method getMaximumCueSize returns the maximumCueSize of this transitionHandler object.
     *
     * @return the maximumCueSize (type int) of this transitionHandler object.
     */
    public int getMaximumCueSize() {
        return 10000;
    }

    /**
     * Method getSkipSize returns the skipSize of this transitionHandler object.
     *
     * @return the skipSize (type int) of this transitionHandler object.
     */
    public int getSkipSize() {
        return 1;
    }

    /**
     * Method maySkipMessages ...
     *
     * @return boolean
     */
    public boolean maySkipMessages() {
        return false;
    }

    /**
     * Method mustSerializeMessages ...
     *
     * @return boolean
     */
    public boolean mustSerializeMessages() {
        return true;
    }

    /**
     * Method getMaximumThreads returns the maximumThreads of this transitionHandler object.
     *
     * @return the maximumThreads (type int) of this transitionHandler object.
     */
    public int getMaximumThreads() {
        return 200;
    }

    /**
     * Method getMessagesPerThread returns the messagesPerThread of this transitionHandler object.
     *
     * @return the messagesPerThread (type int) of this transitionHandler object.
     */
    public int getMessagesPerThread() {
        return 1;
    }

}
