package org.jlab.coda.emu.support.data;

/**
 * Interface used to designate the presence of an attached object.
 * @author timmer
 * 5/16/13
 */
public interface Attached {
    /**
     * This method gets an attached object.
     * @return the attached object.
     */
    public Object getAttachment();

    /**
     * This method sets an attached object.
     * @param attachment the object to attach.
     */
    public void setAttachment(Object attachment);
}
