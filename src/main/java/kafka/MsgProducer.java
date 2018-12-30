package kafka;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;


import org.apache.kafka.clients.producer.*;

/**
 * Created by Administrator on 2018/12/27.
 */
public class MsgProducer {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        //配置文件对象创建
        Properties props = new Properties();
        //kafka的brokers列表
        props.put("bootstrap.servers", "192.168.153.130:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        //发送的消息，按照指定序列化进行
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        //kafka发送者
        Producer<String, String> producer = new KafkaProducer<String, String>(props);
        for (int i=0; i<5; i++){
            //public ProducerRecord(String topic, Integer partition, Long timestamp, K key, V value, Iterable<Header> headers)
            //<1> 若指定Partition ID,则PR被发送至指定Partition
            //<2> 若未指定Partition ID,但指定了Key, PR会按照hasy(key)发送至对应Partition
            //<3> 若既未指定Partition ID也没指定Key，PR会按照round-robin模式发送到每个Partition
            //<4> 若同时指定了Partition ID和Key, PR只会发送到指定的Partition (Key不起作用，代码逻辑决定)
            //同步方式发送消息
            ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>("kafka-test", Integer.toString(i));
//            Future<RecordMetadata> result = producer.send(producerRecord);
//			//等待消息发送成功的同步阻塞方法
//			RecordMetadata metadata = result.get();
//			System.out.println("同步方式发送消息结果：" + "topic-" + metadata.topic() + "|partition-"
//			        + metadata.partition() + "|offset-" + metadata.offset());

            //异步方式发送消息
            producer.send(producerRecord, new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    if (exception != null) {
                        System.err.println("发送消息失败：" + exception.getStackTrace());

                    }
                    if (metadata != null) {
                        System.out.println("异步方式发送消息结果：" + "topic-" + metadata.topic() + "|partition-"
                                + metadata.partition() + "|offset-" + metadata.offset());
                    }
                }
            });
        }
//        producer.close();
    }
}
