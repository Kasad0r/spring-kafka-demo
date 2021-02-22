package kasad0r.kafka.kafkademo;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.stereotype.Component;

import java.io.Closeable;
import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
@Component
public class MyProducer implements Closeable {
    private String topic;
    private KafkaProducer<String, String> producer = getProducer();

    public MyProducer(String topic) {
        this.topic = topic;
    }

    private KafkaProducer<String, String> getProducer() {
        var props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.setProperty(ProducerConfig.CLIENT_ID_CONFIG, "client_id");
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return new KafkaProducer<>(props);
    }

    public void send(String key, String val) throws ExecutionException, InterruptedException {
        producer.send(new ProducerRecord<>(topic, key, val)).get();

    }

    @Override
    public void close() throws IOException {
        producer.close();
    }
}
