package org.jlab.coda.support.evio;

/**
 * Created by IntelliJ IDEA.
 * User: heyes
 * Date: Dec 12, 2008
 * Time: 11:11:40 AM
 * To change this template use File | Settings | File Templates.
 */
public interface DataObject {

    byte[] getBuffer();

    void setBuffer(byte[] data);

    int getBufferLength();

    byte[] getData();

    int length();

    void setLength(int length);

    int getHeader();

    void setHeader(int recordHeader);

    int getStart();

    void setStart(int start);

    void setN(int n) throws BankFormatException;

    int getN() throws BankFormatException;

    int getStat() throws BankFormatException;

    int getId();

    void dumpHeader();

    void setId(int id);

    void setStat(int s) throws BankFormatException;

    void setPayloadClass(Class c);

    Class getPayloadClass();

    int getCursor();

    void setCursor(int end);

    void add(DataObject o) throws BankFormatException;

    void add(int d) throws BankFormatException;
}
