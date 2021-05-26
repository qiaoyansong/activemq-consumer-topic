package util;

import org.apache.activemq.ActiveMQXAConnectionFactory;

import javax.jms.JMSException;
import javax.jms.XAConnection;

/**
 * @author ：Qiao Yansong
 * @date ：Created in 2021/5/26 9:33
 * description：
 * 基本与mybatis获取Session的方式一致
 */
public class ActiveMqUtil {

    private static final String ACTIVE_MQ_URL = "tcp://192.168.40.128:61616";

    private ActiveMqUtil() {

    }

    public static XAConnection getConnection() throws JMSException {
        ActiveMQXAConnectionFactory factory = new ActiveMQXAConnectionFactory(ACTIVE_MQ_URL);
        XAConnection xaConnection = factory.createXAConnection();
        return xaConnection;
    }
}
