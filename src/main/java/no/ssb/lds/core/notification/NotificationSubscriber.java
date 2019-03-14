package no.ssb.lds.core.notification;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import static no.ssb.lds.core.notification.KafkaConfigurator.*;

public class NotificationSubscriber {

    private static final Logger LOG = LoggerFactory.getLogger(NotificationSubscriber.class);

    public KafkaConsumer subscribeToMultipleTopics(List<String> topics) {
        Properties prop = setConsumer();
        KafkaConsumer subscriber = new KafkaConsumer(prop);
        subscriber.subscribe(topics);
        return subscriber;
    }

    public KafkaConsumer subscribeToSingleTopic(String topic) {
        Properties prop = setConsumer();
        KafkaConsumer subscriber = new KafkaConsumer(prop);
        subscriber.subscribe(Collections.singletonList(topic));
        return subscriber;
    }

    public void consumeNotification(KafkaConsumer subscriber) {
        ConsumerRecords<String, String> consumerRecords = subscriber.poll(Duration.ofMillis(15000));
        consumerRecords.forEach(record -> {
            LOG.info("Consuming notification: ",record.value());
        });
    }
}
