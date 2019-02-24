package net.intelie.challenges;

import org.apache.commons.lang3.reflect.FieldUtils;
import org.junit.Test;

import static org.assertj.core.api.Assertions.*;

import java.util.concurrent.ConcurrentHashMap;

public class EventStoreImplTest {

    @Test
    public void insert() throws IllegalAccessException {
        EventStoreImpl es = new EventStoreImpl();

        ConcurrentHashMap<String, EventStoreImpl.NodeControl> events = (ConcurrentHashMap<String, EventStoreImpl.NodeControl>)
                FieldUtils.getDeclaredField(EventStoreImpl.class, "events", true).get(es);

        assertThat(events).isEmpty();

        Event evt1 = new Event("type01", 1);
        Event evt2 = new Event("type01", 2);
        Event evt3 = new Event("type02", 3);
        Event evt4 = new Event("type02", 4);
        Event evt5 = new Event("type01", 3);

        es.insert(evt1);

        assertThat(events).containsOnlyKeys("type01");

        EventStoreImpl.NodeControl type01_nodecontroll = events.get("type01");

        assertThat(type01_nodecontroll).isNotNull();

        assertThat(type01_nodecontroll.length).isEqualTo(1);
        assertThat(type01_nodecontroll.first).isNotNull();
        assertThat(type01_nodecontroll.last).isNotNull();
        assertThat(type01_nodecontroll.first.event).isEqualTo(evt1);
        assertThat(type01_nodecontroll.last.event).isEqualTo(evt1);


        es.insert(evt2);
        assertThat(type01_nodecontroll.length).isEqualTo(2);
        assertThat(type01_nodecontroll.first.event).isEqualTo(evt1);
        assertThat(type01_nodecontroll.last.event).isEqualTo(evt2);
        assertThat(type01_nodecontroll.first.next.event).isEqualTo(evt2);
        assertThat(type01_nodecontroll.last.prev.event).isEqualTo(evt1);

        assertThat(type01_nodecontroll.first.prev).isNull();
        assertThat(type01_nodecontroll.last.next).isNull();

        es.insert(evt3);
        assertThat(events).containsOnlyKeys("type01", "type02");

        EventStoreImpl.NodeControl type02_nodecontroll = events.get("type02");

        assertThat(type02_nodecontroll).isNotNull();
        assertThat(type02_nodecontroll.length).isEqualTo(1);
        assertThat(type02_nodecontroll.first).isNotNull();
        assertThat(type02_nodecontroll.last).isNotNull();
        assertThat(type02_nodecontroll.first.event).isEqualTo(evt3);
        assertThat(type02_nodecontroll.last.event).isEqualTo(evt3);

        es.insert(evt4);
        assertThat(type02_nodecontroll.length).isEqualTo(2);
        assertThat(type02_nodecontroll.first.event).isEqualTo(evt3);
        assertThat(type02_nodecontroll.last.event).isEqualTo(evt4);
        assertThat(type02_nodecontroll.first.next.event).isEqualTo(evt4);
        assertThat(type02_nodecontroll.last.prev.event).isEqualTo(evt3);

        es.insert(evt5);
        assertThat(type01_nodecontroll.length).isEqualTo(3);
        assertThat(type01_nodecontroll.first.event).isEqualTo(evt1);
        assertThat(type01_nodecontroll.last.event).isEqualTo(evt5);
        assertThat(type01_nodecontroll.first.next.event).isEqualTo(evt2);
        assertThat(type01_nodecontroll.first.next.next.event).isEqualTo(evt5);
        assertThat(type01_nodecontroll.last.prev.event).isEqualTo(evt2);
        assertThat(type01_nodecontroll.last.prev.prev.event).isEqualTo(evt1);

        assertThat(type01_nodecontroll.first.prev).isNull();
        assertThat(type01_nodecontroll.last.next).isNull();
    }

    @Test
    public void removeAll() throws IllegalAccessException {
        EventStoreImpl es = new EventStoreImpl();
        ConcurrentHashMap<String, EventStoreImpl.NodeControl> events = (ConcurrentHashMap<String, EventStoreImpl.NodeControl>)
                FieldUtils.getDeclaredField(EventStoreImpl.class, "events", true).get(es);

        assertThat(events).isEmpty();

        es.insert(new Event("type", 1));
        es.insert(new Event("type", 1));
        assertThat(events).isNotEmpty();

        assertThat(events.get("type").length).isEqualTo(2);

        es.removeAll("type");
        assertThat(events.get("type").length).isEqualTo(0);
    }

    @Test
    public void query() {
        EventStoreImpl es = new EventStoreImpl();

        Event evt1 = new Event("type", 1);
        Event evt2 = new Event("type", 2);
        Event evt3 = new Event("type", 2);
        Event evt4 = new Event("type", 3);

        es.insert(evt1);
        es.insert(evt2);
        es.insert(evt3);
        es.insert(evt4);

        EventIterator query1 = es.query("type", 2, 3);

        assertThat(query1.moveNext()).isTrue();
        assertThat(query1.current()).isEqualTo(evt2);
        assertThat(query1.moveNext()).isTrue();
        assertThat(query1.current()).isEqualTo(evt3);
        assertThat(query1.moveNext()).isFalse();

        EventIterator query2 = es.query("type", 1, 4);

        assertThat(query2.moveNext()).isTrue();
        assertThat(query2.current()).isEqualTo(evt1);
        assertThat(query2.moveNext()).isTrue();
        assertThat(query2.current()).isEqualTo(evt2);
        assertThat(query2.moveNext()).isTrue();
        assertThat(query2.current()).isEqualTo(evt3);
        assertThat(query2.moveNext()).isTrue();
        assertThat(query2.current()).isEqualTo(evt4);

        assertThat(query2.moveNext()).isFalse();

        testIllegalStateException(query2);


        Event evt5 = new Event("type", 3);
        es.insert(evt5);

        assertThat(query2.moveNext()).isTrue();
        assertThat(query2.current()).isEqualTo(evt5);

        es.insert(new Event("type", 4));
        assertThat(query2.moveNext()).isFalse();
    }

    @Test
    public void queryShouldCallMoveNext() {
        EventStoreImpl es = new EventStoreImpl();

        EventIterator query1 = es.query("type", 1, 4);

        testIllegalStateException(query1);

        assertThat(query1.moveNext()).isFalse();
    }


    @Test
    public void queryRemove() throws IllegalAccessException {
        EventStoreImpl es = new EventStoreImpl();

        ConcurrentHashMap<String, EventStoreImpl.NodeControl> events = (ConcurrentHashMap<String, EventStoreImpl.NodeControl>)
                FieldUtils.getDeclaredField(EventStoreImpl.class, "events", true).get(es);


        Event evt1 = new Event("type", 10);
        Event evt2 = new Event("type", 20);
        Event evt3 = new Event("type", 25);
        Event evt4 = new Event("type", 30);
        Event evt5 = new Event("type", 35);
        Event evt6 = new Event("type", 38);

        es.insert(evt1);
        es.insert(evt2);
        es.insert(evt3);
        es.insert(evt4);
        es.insert(evt5);
        es.insert(evt6);

        EventStoreImpl.NodeControl nodecontrol = events.get("type");
        EventIterator query1 = es.query("type", 10, 40);

        query1.moveNext();
        assertThat(query1.current()).isEqualTo(nodecontrol.first.event);

        // test removing the first
        assertThat(nodecontrol.first.event).isEqualTo(evt1);
        query1.remove();
        testIllegalStateException(query1);

        assertThat(nodecontrol.first.event).isEqualTo(evt2);
        assertThat(nodecontrol.first.prev).isNull();
        assertThat(nodecontrol.first.next.event).isEqualTo(evt3);

        query1.moveNext();
        assertThat(query1.current()).isEqualTo(evt2).isEqualTo(nodecontrol.first.event);

        query1.moveNext();
        assertThat(query1.current()).isEqualTo(evt3);
        query1.remove();
        testIllegalStateException(query1);

        assertThat(nodecontrol.first.next.event).isEqualTo(evt4);
        assertThat(nodecontrol.first.next.next.event).isEqualTo(evt5);
        assertThat(nodecontrol.last.prev.prev.event).isEqualTo(evt4);
        assertThat(nodecontrol.last.prev.prev.prev.event).isEqualTo(evt2);

        query1.moveNext();
        assertThat(query1.current()).isEqualTo(evt4);
        query1.moveNext();
        assertThat(query1.current()).isEqualTo(evt5);

        // test removing the last
        query1.moveNext();
        assertThat(query1.current()).isEqualTo(evt6).isEqualTo(nodecontrol.last.event);

        query1.remove();
        testIllegalStateException(query1);

        assertThat(nodecontrol.last.event).isEqualTo(evt5);
        assertThat(nodecontrol.last.next).isNull();
        assertThat(nodecontrol.last.prev.event).isEqualTo(evt4);

        assertThat(query1.moveNext()).isFalse();
    }

    private void testIllegalStateException(EventIterator query) {
        assertThatThrownBy(query::current)
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Method should be accessed only after moveNext() == true");

        assertThatThrownBy(query::remove)
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Method should be accessed only after moveNext() == true");
    }
}