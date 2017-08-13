package cn.cherish.learn.rocketmq.broadcasting;

import cn.cherish.learn.rocketmq.RocketMQUtil;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;

/**
 * @author Cherish
 * @version 1.0
 * @date 2017/8/12 20:43
 */
public class BroadcastProducer {
    public static void main(String[] args) throws Exception {
        DefaultMQProducer producer = new DefaultMQProducer(RocketMQUtil.ProducerGroup);
        producer.setNamesrvAddr(RocketMQUtil.NamesrvAddr);
        producer.setInstanceName("BroadcastProducer");
        producer.setVipChannelEnabled(false);
        producer.start();

        for (int i = 0; i < 100; i++){
            Message msg = new Message("BroadcastTest",
                    "TagA",
                    "OrderID" + i,
                    ("Hello world" + i).getBytes(RemotingHelper.DEFAULT_CHARSET));
            SendResult sendResult = producer.send(msg);
            System.out.printf("%s%n", sendResult);
        }
        producer.shutdown();
    }
}
