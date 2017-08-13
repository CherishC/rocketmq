package cn.cherish.learn.rocketmq.schedule;

import cn.cherish.learn.rocketmq.RocketMQUtil;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.List;

/**
 * Start consumer to wait for incoming subscribed messages
 * @author Cherish
 * @version 1.0
 * @date 2017/8/12 20:52
 */
public class ScheduledMessageConsumer {
    public static void main(String[] args) throws Exception {
        // Instantiate message consumer
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(RocketMQUtil.ConsumerGroup);
        consumer.setNamesrvAddr(RocketMQUtil.NamesrvAddr);
        consumer.setInstanceName("ScheduledMessageConsumer");
        consumer.setVipChannelEnabled(false);
        
        // Subscribe topics
        consumer.subscribe("TestScheduled", "*");
        // Register message listener
        consumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(
                    List<MessageExt> messages, ConsumeConcurrentlyContext context) {

                System.out.println("mq = " + context.getMessageQueue());
                for (MessageExt message : messages) {
                    // Print approximate delay time period
                    System.out.println("Receive message[msgId=" + message.getMsgId() + "] "
                            + (System.currentTimeMillis() - message.getStoreTimestamp()) + "ms later");
                }
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });
        // Launch consumer
        consumer.start();
    }
}
