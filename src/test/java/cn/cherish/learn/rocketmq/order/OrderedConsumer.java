package cn.cherish.learn.rocketmq.order;

import cn.cherish.learn.rocketmq.RocketMQUtil;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerOrderly;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.List;

/**
 * @author Cherish
 * @version 1.0
 * @date 2017/8/12 19:53
 */
public class OrderedConsumer {
    public static void main(String[] args) throws Exception {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(RocketMQUtil.ConsumerGroup);

        consumer.setNamesrvAddr(RocketMQUtil.NamesrvAddr);
        consumer.setInstanceName("OrderedConsumer");
        consumer.setVipChannelEnabled(false);

        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        consumer.subscribe("TopicTestOrder", "TagA || TagC || TagD");

        // 实现了MessageListenerOrderly表示一个队列只会被一个线程取到
        consumer.registerMessageListener(new MessageListenerOrderly() {

            @Override
            public ConsumeOrderlyStatus consumeMessage(List<MessageExt> msgs,
                                                       ConsumeOrderlyContext context) {
                context.setAutoCommit(true);
                System.out.printf(Thread.currentThread().getName()
                        + " Receive New Messages: " + msgs + "%n");
                return ConsumeOrderlyStatus.SUCCESS;

            }
        });

        consumer.start();
        System.out.printf("Consumer Started.%n");
    }
}
