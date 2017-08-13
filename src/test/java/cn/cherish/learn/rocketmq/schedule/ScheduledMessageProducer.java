package cn.cherish.learn.rocketmq.schedule;

import cn.cherish.learn.rocketmq.RocketMQUtil;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.message.Message;

/**
 * Send scheduled messages
 * @author Cherish
 * @version 1.0
 * @date 2017/8/12 21:04
 */
public class ScheduledMessageProducer {

    public static void main(String[] args) throws Exception {
        // Instantiate a producer to send scheduled messages
        DefaultMQProducer producer = new DefaultMQProducer(RocketMQUtil.ProducerGroup);
        producer.setNamesrvAddr(RocketMQUtil.NamesrvAddr);
        producer.setInstanceName("ScheduledMessageProducer");
        producer.setVipChannelEnabled(false);

        // Launch producer
        producer.start();
        for (int i = 0; i < 100; i++) {
            Message message = new Message(
                    "TestScheduled",
                    ("Hello scheduled message " + i).getBytes());
            // This message will be delivered to consumer 10 seconds later.
            message.setDelayTimeLevel(3);
            // Send the message
            producer.send(message);
            System.out.println("message = " + message);

            Thread.sleep(1000L);
        }

        // Shutdown producer after use.
        producer.shutdown();
    }
}
