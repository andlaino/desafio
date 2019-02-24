package net.intelie.challenges;

import org.apache.commons.lang3.reflect.FieldUtils;
import org.junit.Test;

import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.*;


public class EventStoreImplConcurrencyTest {
    private static int THREAD_COUNT = 30;
    private static long SAMPLE_COUNT = 300001;

    @Test
    public void insertQueryAndRemove() throws IllegalAccessException, InterruptedException {
        EventStoreImpl es = new EventStoreImpl();

        ConcurrentHashMap<String, EventStoreImpl.NodeControl> events = (ConcurrentHashMap<String, EventStoreImpl.NodeControl>)
                FieldUtils.getDeclaredField(EventStoreImpl.class, "events", true).get(es);

        ExecutorService exs = Executors.newFixedThreadPool(THREAD_COUNT);

        for(int i =1; i <= SAMPLE_COUNT; i++){
            final int ifinal = i;
            exs.submit(() -> {
                es.insert(new Event("type", ifinal));
            });
        }

        exs.shutdown();
        assertThat(exs.awaitTermination(60, TimeUnit.SECONDS)).isTrue();

        EventIterator query = es.query("type", 0, SAMPLE_COUNT + 1);

        // https://pt.wikipedia.org/wiki/Soma_de_uma_progress%C3%A3o_aritm%C3%A9tica
        long expected_sum = (SAMPLE_COUNT * (SAMPLE_COUNT + 1)) / 2;

        long event_count = 0;
        long sum = 0;
        while(query.moveNext()) {
            sum += query.current().timestamp();
            event_count++;
        }

        assertThat(sum).isEqualTo(expected_sum);
        assertThat(events.get("type").length)
                .isEqualTo(event_count)
                .isEqualTo(SAMPLE_COUNT);

        /// Test removing

        exs = Executors.newFixedThreadPool(THREAD_COUNT);

        final long remove_queries_count = 100;
        final long remove_call_count = 20;

        final Object obj = "";

        EventIterator q = es.query("type", 0, SAMPLE_COUNT);
        for(int i =1; i <= remove_queries_count; i++){
            exs.submit(() -> {
                for(int j =0; j < remove_call_count; j++){
                    synchronized (obj) {
                        q.moveNext();
                        q.remove();
                    }
                }
            });
        }

        exs.shutdown();
        assertThat(exs.awaitTermination(60, TimeUnit.SECONDS)).isTrue();

        long k = remove_queries_count * remove_call_count;
        long expected_removed = (k * (k + 1)) / 2;

        query = es.query("type", 0, SAMPLE_COUNT + 1);

        event_count = 0;
        sum = 0;

        EventStoreImpl.EventNode eventNode = events.get("type").first;
        while(query.moveNext()) {
            assertThat(query.current()).isEqualTo(eventNode.event);

            sum += query.current().timestamp();
            event_count++;

            eventNode = eventNode.next;
        }

        assertThat(eventNode).isNull();

        assertThat(sum).isEqualTo(expected_sum - expected_removed);
        assertThat(events.get("type").length)
                .isEqualTo(SAMPLE_COUNT - k)
                .isEqualTo(event_count);
    }
}