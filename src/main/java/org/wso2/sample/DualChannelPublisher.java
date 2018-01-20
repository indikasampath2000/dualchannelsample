package org.wso2.sample;

import javax.jms.JMSException;
import javax.jms.Queue;
import javax.jms.QueueConnection;
import javax.jms.QueueConnectionFactory;
import javax.jms.QueueSender;
import javax.jms.QueueSession;
import javax.jms.TextMessage;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import java.util.Properties;

public class DualChannelPublisher {

    public static final String QPID_ICF = "org.wso2.andes.jndi.PropertiesFileInitialContextFactory";
    private static final String CF_NAME_PREFIX = "connectionfactory.";
    private static final String QUEUE_NAME_PREFIX = "queue.";
    private static final String CF_NAME = "qpidConnectionfactory";
    private String queueName = "replyQueue";
    private QueueConnection queueConnection;
    private QueueSession queueSession;
    private QueueSender queueSender;


    public DualChannelPublisher() throws NamingException, JMSException {
        Properties properties = new Properties();
        properties.put(Context.INITIAL_CONTEXT_FACTORY, QPID_ICF);
        properties.put(CF_NAME_PREFIX + CF_NAME, getTCPConnectionURL());
        properties.put(QUEUE_NAME_PREFIX + queueName, queueName);
        InitialContext ctx = new InitialContext(properties);
        // Lookup connection factory
        QueueConnectionFactory connFactory = (QueueConnectionFactory) ctx.lookup(CF_NAME);
        queueConnection = connFactory.createQueueConnection();
        queueConnection.start();
        queueSession = queueConnection.createQueueSession(false, QueueSession.AUTO_ACKNOWLEDGE);
        // Send message
        Queue queue = (Queue)ctx.lookup(queueName);
        queueSender = queueSession.createSender(queue);
    }

    public void sendMessages(TextMessage textMessage) throws JMSException {
        queueSender.send(textMessage);
    }

    private String getTCPConnectionURL() {
        // amqp://{username}:{password}@carbon/carbon?brokerlist='tcp://{hostname}:{port}'
        return "amqp://admin:admin@carbon/carbon?failover='roundrobin'&cyclecount='2'&brokerlist='tcp://localhost:8672?retries='5'&connectdelay='10000'&ssl='true'&trust_store='/home/indika/dev-zone/dev-service/FOIIT/setup/mb-cluster/node1/wso2mb-3.2.0/repository/resources/security/client-truststore.jks'&trust_store_password='wso2carbon'&key_store='/home/indika/dev-zone/dev-service/FOIIT/setup/mb-cluster/node1/wso2mb-3.2.0/repository/resources/security/client-truststore.jks'&key_store_password='wso2carbon'&ssl_cert_alias='wso2carbon';tcp://localhost:8673?retries='5'&connectdelay='10000'&ssl='true'&trust_store='/home/indika/dev-zone/dev-service/FOIIT/setup/mb-cluster/node2/wso2mb-3.2.0/repository/resources/security/client-truststore.jks'&trust_store_password='wso2carbon'&key_store='/home/indika/dev-zone/dev-service/FOIIT/setup/mb-cluster/node2/wso2mb-3.2.0/repository/resources/security/client-truststore.jks'&key_store_password='wso2carbon'&ssl_cert_alias='wso2carbon''";
    }

    public QueueConnection getQueueConnection() {
        return queueConnection;
    }

    public QueueSession getQueueSession() {
        return queueSession;
    }

    public QueueSender getQueueSender() {
        return queueSender;
    }
}
