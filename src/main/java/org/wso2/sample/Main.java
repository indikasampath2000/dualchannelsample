package org.wso2.sample;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.JMSException;
import javax.naming.NamingException;

public class Main {

    private static Logger log = LoggerFactory.getLogger(DualChannelConsumer.class);

    public static void main(String[] args) {
        DualChannelConsumer dualChannelConsumer = new DualChannelConsumer();
        try {
            dualChannelConsumer.receiveMessages();
        } catch (NamingException | JMSException e) {
            log.error("Error occurred.", e);
        }
    }
}
