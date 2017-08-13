package cn.cherish.learn.rocketmq.order;

import cn.cherish.learn.rocketmq.RocketMQUtil;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.MessageQueueSelector;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.common.RemotingHelper;

import java.util.List;

/**
 * 发送顺序消息
 * @author Cherish
 * @version 1.0
 * @date 2017/8/12 19:51
 */
public class OrderedProducer {

    public static void main(String[] args) throws Exception {
        //Instantiate with a producer group name.
        DefaultMQProducer producer = new DefaultMQProducer(RocketMQUtil.ProducerGroup);
        producer.setNamesrvAddr(RocketMQUtil.NamesrvAddr);
        producer.setInstanceName("OrderedProducer");
        producer.setVipChannelEnabled(false);
        //Launch the instance.
        producer.start();
        String[] tags = new String[] {"TagA", "TagB", "TagC", "TagD", "TagE"};
        for (int i = 0; i < 100; i++) {
            int orderId = i % 10;
            //Create a message instance, specifying topic, tag and message body.
            Message msg = new Message(
                    "TopicTestOrder",
                    tags[i % tags.length],
                    "KEY" + i,
                    ("Hello RocketMQ " + i).getBytes(RemotingHelper.DEFAULT_CHARSET));
            SendResult sendResult = producer.send(msg, new MessageQueueSelector() {
                @Override
                public MessageQueue select(List<MessageQueue> mqs, Message msg, Object arg) {
                    Integer id = (Integer) arg;
                    int index = id % mqs.size();
                    return mqs.get(index);
                }
            }, orderId);

            System.out.println(sendResult);
        }
        //server shutdown
        producer.shutdown();
    }
}
