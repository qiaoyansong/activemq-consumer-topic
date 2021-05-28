package util;

import org.apache.activemq.ActiveMQConnectionFactory;

import javax.jms.Connection;
import javax.jms.JMSException;

/**
 * @author ：Qiao Yansong
 * @date ：Created in 2021/5/28 11:02
 * description：
 */
public class ActiveMqUtil {
    private static final String ACTIVE_MQ_URL = "tcp://192.168.40.128:61616";

    private ActiveMqUtil() {

    }

    public static Connection getConnection() throws JMSException {
        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(ACTIVE_MQ_URL);
        Connection connection = factory.createConnection();
        return connection;
    }
}
