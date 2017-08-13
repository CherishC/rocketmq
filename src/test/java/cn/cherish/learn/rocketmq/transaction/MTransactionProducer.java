package cn.cherish.learn.rocketmq.transaction;

import cn.cherish.learn.rocketmq.RocketMQUtil;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.*;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;

/**
 * @author Cherish
 * @version 1.0
 * @date 2017/8/12 22:31
 */
public class MTransactionProducer {

    public static void main(String[] args) throws MQClientException, InterruptedException {

        TransactionMQProducer producer = new TransactionMQProducer("transaction_Producer");
        producer.setNamesrvAddr(RocketMQUtil.NamesrvAddr);
        producer.setInstanceName("MTransactionProducer");
        producer.setVipChannelEnabled(false);
        // 事务回查最小并发数
        producer.setCheckThreadPoolMinSize(2);
        // 事务回查最大并发数
        producer.setCheckThreadPoolMaxSize(2);
        // 队列数
        producer.setCheckRequestHoldMax(2000);
        producer.setTransactionCheckListener(new TransactionCheckListenerImpl());
        producer.start();

        // String[] tags = new String[] { "TagA", "TagB", "TagC", "TagD", "TagE" };
        TransactionExecuterImpl tranExecuter = new TransactionExecuterImpl();

        for (int i = 1; i <= 3; i++) {
            try {
                Message msg = new Message(
                        "TopicTransactionTest",
                        "transaction" + i,
                        "KEY" + i,
                        ("Hello RocketMQ " + i).getBytes());
                TransactionSendResult sendResult = producer.sendMessageInTransaction(msg, tranExecuter, "CherishTransactionMQ");
                System.out.println(sendResult);
                System.out.println(sendResult.getLocalTransactionState());

                Thread.sleep(10);
            } catch (MQClientException e) {
                e.printStackTrace();
            }
        }

        for (int i = 50; i <= 99; i++) {
            try {
                Message msg = new Message(
                        "TopicTransactionTest",
                        "transaction" + i,
                        "KEY" + i,
                        ("Hello RocketMQ " + i).getBytes());
                TransactionSendResult sendResult = producer.sendMessageInTransaction(msg, tranExecuter, "CherishTransactionMQ");
                System.out.println(sendResult);
                System.out.println(sendResult.getLocalTransactionState());

                Thread.sleep(5000);
            } catch (MQClientException e) {
                e.printStackTrace();
            }
        }

        Thread.sleep(100_1000);

        producer.shutdown();
    }


    /**
     * 执行本地事务
     */
    static class TransactionExecuterImpl implements LocalTransactionExecuter {
        // private AtomicInteger transactionIndex = new AtomicInteger(1);

        public LocalTransactionState executeLocalTransactionBranch(final Message msg, final Object arg) {

            System.out.println("执行本地事务msg = " + new String(msg.getBody()));
            System.out.println("执行本地事务arg = " + arg);

            String tags = msg.getTags();

            if (tags.equals("transaction2")) {
                System.out.println("======我的操作==，失败了  -进行ROLLBACK");
                return LocalTransactionState.ROLLBACK_MESSAGE;
            }
            if (msg.getTags().equals("transaction3")) {
                System.out.println("======transaction3  抛异常");
                throw new RuntimeException("transaction3  抛异常");
            }

            return LocalTransactionState.COMMIT_MESSAGE;
            // return LocalTransactionState.UNKNOW;
        }
    }

    /**
     * 未决事务，服务器回查客户端
     */
    static class TransactionCheckListenerImpl implements TransactionCheckListener {
        // private AtomicInteger transactionIndex = new AtomicInteger(0);

        // 在这里，我们可以根据由MQ回传的key去数据库查询，这条数据到底是成功了还是失败了。
        public LocalTransactionState checkLocalTransactionState(MessageExt msg) {
            System.out.println("msg = " + msg);
            System.out.println("未决事务，服务器回查客户端msg =" + new String(msg.getBody()));

            if (msg.getTags().equals("transaction2")) {
                System.out.println("TransactionCheckListenerImpl 失败了-进行ROLLBACK");
                return LocalTransactionState.ROLLBACK_MESSAGE;
            }

            System.out.println("============ COMMIT_MESSAGE ===========");
            // 逻辑。。。。。
            return LocalTransactionState.COMMIT_MESSAGE;
        }
    }
}
