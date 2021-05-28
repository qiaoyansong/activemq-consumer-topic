package main;

import util.ActiveMqUtil;

import javax.jms.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @author ：Qiao Yansong
 * @date ：Created in 2021/5/26 9:39
 * description：
 */
public class Main {

    private static final String TOPIC_NAME = "topic1";

    public static void main(String[] args) {
        testConsumerUseReceive();
    }

    /**
     * 两个消费者查看消费的信息
     */
    private static void testParallel() {
        ExecutorService THREAD_POOl = Executors.newFixedThreadPool(2);
        try {
            for (int i = 0; i < 2; i++) {
                THREAD_POOl.execute(() -> {
                    System.out.println("这是消费者" + Thread.currentThread().getName());
                    Connection connection = null;
                    Session session = null;
                    MessageConsumer consumer = null;
                    try {
                        connection = ActiveMqUtil.getConnection();
                        connection.start();
                        session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
                        Topic topic = session.createTopic(TOPIC_NAME);
                        consumer = session.createConsumer(topic);
                        while (true) {
                            TextMessage message = (TextMessage) consumer.receive();
                            System.out.println("消费者" + Thread.currentThread().getName() + "消费的消息是" + message.getText());
                        }

                    } catch (JMSException e) {
                        e.printStackTrace();
                    } finally {
                        if (consumer != null) {
                            try {
                                consumer.close();
                            } catch (JMSException e) {
                                e.printStackTrace();
                            }
                        }
                        if (session != null) {
                            try {
                                session.close();
                            } catch (JMSException e) {
                                e.printStackTrace();
                            }
                        }
                        if (connection != null) {
                            try {
                                connection.close();
                            } catch (JMSException e) {
                                e.printStackTrace();
                            }
                        }
                    }
                });
            }
        } finally {
            if (THREAD_POOl != null) {
                THREAD_POOl.shutdown();
            }
        }
    }

    /**
     * 消费者使用receive接受消息
     */
    private static void testConsumerUseReceive() {
        Connection connection = null;
        Session session = null;
        MessageConsumer consumer = null;
        try {
            connection = ActiveMqUtil.getConnection();
            connection.start();
            session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Topic topic = session.createTopic(TOPIC_NAME);
            consumer = session.createConsumer(topic);
            while (true) {
                TextMessage message = (TextMessage) consumer.receive();
                System.out.println("消费的消息是" + message.getText());
            }

        } catch (JMSException e) {
            e.printStackTrace();
        } finally {
            if (consumer != null) {
                try {
                    consumer.close();
                } catch (JMSException e) {
                    e.printStackTrace();
                }
            }
            if (session != null) {
                try {
                    session.close();
                } catch (JMSException e) {
                    e.printStackTrace();
                }
            }
            if (connection != null) {
                try {
                    connection.close();
                } catch (JMSException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    /**
     * 消费者使用receive接受消息，并且支持持久化
     */
    private static void testConsumerUseReceiveWithPersistent() {
        Connection connection = null;
        Session session = null;
        TopicSubscriber durableSubscriber = null;
        try {
            connection = ActiveMqUtil.getConnection();
            connection.setClientID("client1");
            session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Topic topic = session.createTopic(TOPIC_NAME);
            durableSubscriber = session.createDurableSubscriber(topic, "client1-sub");
            connection.start();
            while (true) {
                TextMessage message = (TextMessage) durableSubscriber.receive();
                System.out.println("消费的消息是" + message.getText());
            }

        } catch (JMSException e) {
            e.printStackTrace();
        } finally {
            if (durableSubscriber != null) {
                try {
                    durableSubscriber.close();
                } catch (JMSException e) {
                    e.printStackTrace();
                }
            }
            if (session != null) {
                try {
                    session.close();
                } catch (JMSException e) {
                    e.printStackTrace();
                }
            }
            if (connection != null) {
                try {
                    connection.close();
                } catch (JMSException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
