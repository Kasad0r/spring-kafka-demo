package kasad0r.kafka.kafkademo;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.Closeable;
import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.function.Consumer;

public class MyConsumer implements Closeable {

    private final String topic;
    private final KafkaConsumer<String, String> consumer ;

    public MyConsumer(String topic) {
        this.topic = topic;
        consumer=getConsumer();
    }

    private KafkaConsumer<String, String> getConsumer() {
        var props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "group_id");
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        var consumer = new KafkaConsumer<String, String>(props);
        consumer.subscribe(Collections.singletonList(topic));
        return consumer;
    }

    public void consume(Consumer<ConsumerRecord<String, String>> recordConsumer) {
        new Thread(() -> {
            while (true) {
                var records = consumer.poll(Duration.ofSeconds(1));
                records.forEach(recordConsumer::accept);
            }
        }).start();
    }

    @Override
    public void close() throws IOException {
        consumer.close();
    }
}
