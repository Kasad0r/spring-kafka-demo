package kasad0r.kafka.kafkademo;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.io.IOException;
import java.sql.Time;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

@SpringBootApplication
public class KafkaDemoApplication {


    public static void main(String[] args) throws ExecutionException, InterruptedException, IOException {
        // SpringApplication.run(KafkaDemoApplication.class, args);
        new Thread(() -> {
            MyProducer myProducer = new MyProducer("spring-kafka-demo");
            try {
                myProducer.send("hello", "hello-world");

                for (int i = 0; i < 100; i++) {
                    myProducer.send("" + i, "Hello from producer! ");
                    TimeUnit.SECONDS.sleep(3);
                }
                myProducer.close();
            } catch (ExecutionException | InterruptedException | IOException e) {
                e.printStackTrace();
            }
        }).start();
        var consumer = new MyConsumer("spring-kafka-demo");
        consumer.consume(record -> {
            System.out.println("Got key" + record.key() + "value" + record.value());
        });
        TimeUnit.MINUTES.sleep(5);

        consumer.close();
    }

}
