package company.vk.edu.distrib.compute.vagifbaratov.moderation;

import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.moderation.LinkCreatedEvent;
import company.vk.edu.distrib.compute.moderation.ModerationResultEvent;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Instant;
import java.util.NoSuchElementException;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

public final class LinkModerationClient extends KafkaWorker {
    private static final Logger log = LoggerFactory.getLogger(LinkModerationClient.class);
    private final BlockingQueue<LinkCreatedEvent> pending = new ArrayBlockingQueue<>(1024);
    private final Dao<String> linksDao;

    public LinkModerationClient(String bootstrapServers, Dao<String> linksDao) {
        super(bootstrapServers, "links.moderated", "vagifbaratov-shortener-" + UUID.randomUUID());
        this.linksDao = linksDao;
    }

    public void linkCreated(String id, String url) {
        if (!pending.offer(new LinkCreatedEvent(id, url, Instant.now()))) {
            log.warn("Moderation queue full; link {} stay available", id);
        }
    }

    @Override
    protected void publishPending(Producer<String, String> producer) {
        for (int index = 0; index < 32; index++) {
            LinkCreatedEvent event = pending.peek();
            if (event == null) {
                return;
            }

            var record = new ProducerRecord<>("links.created", event.id(), EventCodecUtils.encode(event));

            producer.send(record, (metadata, exception) -> {
                if (exception != null) {
                    log.warn("Could not publish link {} for moderation", event.id(), exception);
                    if (!pending.offer(event)) {
                        log.warn("Moderation retry queue is full for link {}", event.id());
                    }
                }
            });

            pending.remove();
        }
    }

    @Override
    protected void process(
        ConsumerRecord<String, String> record,
        Producer<String, String> producer
    ) throws IOException {
        ModerationResultEvent result = EventCodecUtils.decodeModerated(record.value());
        if (!result.id().equals(record.key())) {
            throw new IllegalArgumentException("Result key not match link ID");
        }

        if (!result.passed()) {
            try {
                linksDao.delete(result.id());
            } catch (NoSuchElementException expected) {
                // Repeated delivery or link already deleted by user
            }
        }
    }
}
