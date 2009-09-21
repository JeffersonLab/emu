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

package org.jlab.coda.support.ui.log;

import org.jlab.coda.support.logger.Logger;
import org.jlab.coda.support.logger.LoggerAppender;
import org.jlab.coda.support.logger.LoggingEvent;
import org.jlab.coda.support.logger.QueueAppender;

import javax.swing.*;
import javax.swing.text.*;
import java.awt.*;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * <pre>
 * Class <b>SwingLogConsoleDialog </b>
 * </pre>
 *
 * @author heyes
 *         Created on Sep 17, 2008
 */
public class SwingLogConsoleDialog extends JTextPane implements LoggerAppender {

    /** Field serialVersionUID */
    private static final long serialVersionUID = 1L;

    /** Field caret */
    private LogTextCaret caret;

    QueueAppender logQueueAppender;

    Thread monitorThread;

    protected class MonitorRunnable implements Runnable {
        public void run() {

            LoggingEvent event;

            // blocks forever here!!

            while (!monitorThread.isInterrupted()) {
                event = logQueueAppender.poll(1);
                if (event != null) {
                    append(event);
                }
            }

        }
    }

    public SwingLogConsoleDialog() {
        super();
        caret = new LogTextCaret();
        setCaret(caret);
        // this.maxLines = maxLines;
        setEditable(false);

        Style style = addStyle("Red", null);
        StyleConstants.setForeground(style, Color.red);
        style = addStyle("WARN", null);
        StyleConstants.setForeground(style, new Color(200,155,0)); // darker yellow
        style = addStyle("INFO", null);
        StyleConstants.setForeground(style, new Color(0,150,0)); // darker green
        style = addStyle("Gray", null);
        StyleConstants.setForeground(style, Color.gray);
        style = addStyle("DEBUG", null);
        StyleConstants.setForeground(style, Color.blue);
        style = addStyle("ERROR", null);
        StyleConstants.setBold(style, true);
        //StyleConstants.setItalic(style, true);
        //StyleConstants.setFontSize(style, 18);
        StyleConstants.setForeground(style, Color.red);
        style = addStyle("pt18", null);
        StyleConstants.setFontSize(style, 18);
    }

    public void monitor(QueueAppender appender) {
        logQueueAppender = appender;
        monitorThread = new Thread(new MonitorRunnable());
        monitorThread.start();
    }

    public void close() {
        monitorThread.interrupt();
        Logger.removeAppender(logQueueAppender);
    }

    /**
     * Method append ...
     *
     * @param event of type LoggingEvent
     */
    public void append(LoggingEvent event) {
        Document d = getDocument();
        try {
            // add the time in gray
            d.insertString(d.getLength(), formatEventTime(event.getEventTime()), getStyle("Gray"));

            // add type of entry
            switch (event.getLevel()) {
                case LoggingEvent.ERROR:
                    d.insertString(d.getLength(), "  ERROR \t", getStyle("ERROR"));
                    break;
                case LoggingEvent.INFO:
                    d.insertString(d.getLength(), "  INFO \t", getStyle("INFO"));
                    break;
                case LoggingEvent.DEBUG:
                    d.insertString(d.getLength(), "  DEBUG \t", getStyle("DEBUG"));
                    break;
                case LoggingEvent.WARN:
                    d.insertString(d.getLength(), "  WARN \t", getStyle("WARN"));
                    break;
            }

            // add the message
            d.insertString(d.getLength(), event.getMessage(), null);

            // if (event.hasData()) {
            // super.append(" [").append(event.getFormatedData()).append("]");
            // }
            // super.append("\t ").append(formatLocation(event.getLocation()));
            d.insertString(d.getLength(), "\n", getStyle("Gray"));

            JViewport viewport = (JViewport) getParent();
            boolean scrollToBottom = Math.abs(viewport.getViewPosition().getY() - (getHeight() - viewport.getHeight())) < 100;

            caret.setVisibilityAdjustment(scrollToBottom);

            if (scrollToBottom) {
                setCaretPosition(getText().length());
            }
        } catch (BadLocationException e) {
            e.printStackTrace();
        }

    }

    /**
     * Method formatEventTime ...
     *
     * @param eventTime of type long
     * @return String
     */
    private String formatEventTime(long eventTime) {
        //String fullformat = "''yy EEE, MMM dd, HH:mm:ss.SSS ";

        DateFormat format = new SimpleDateFormat("HH:mm:ss.SSS");
        return format.format(new Date(eventTime));
    }

    /**
     * Method append ...
     *
     * @param str of type String
     */
    public void append(String str) {
        Document d = getDocument();
        try {
            d.insertString(d.getLength(), str, null);
        } catch (BadLocationException e) {
            e.printStackTrace();
        }
    }

    /**
     * Method append ...
     *
     * @param str   of type String
     * @param style of type String
     */
    public void append(String str, String style) {
        StyledDocument d = getStyledDocument();

        try {
            int start = d.getLength();
            d.insertString(d.getLength(), str, null);
            d.setCharacterAttributes(start, str.length(), getStyle(style), true);
        } catch (BadLocationException e) {
            e.printStackTrace();
        }
    }

}
