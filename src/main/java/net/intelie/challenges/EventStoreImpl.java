package net.intelie.challenges;

import java.util.concurrent.ConcurrentHashMap;

public class EventStoreImpl implements EventStore {
    private class EventIteratorImpl implements EventIterator{
        private EventNode currentNode;
        private NodeControl nodeControl;
        private boolean has_no_more;
        private final long startTime;
        private final long endTime;

        EventIteratorImpl (NodeControl nodeControl, long startTime, long endTime){
            this.nodeControl = nodeControl;
            this.startTime = startTime;
            this.endTime = endTime;
        }

        @Override
        public boolean moveNext() {
            synchronized (this) {
                if (nodeControl == null) return false;

                do {
                    if (currentNode != null) {
                        if (currentNode.next == null) {
                            has_no_more = true;
                            return false;
                        }

                        currentNode = currentNode.next;
                    } else
                        currentNode = nodeControl.first;

                    if (currentNode.belongs(startTime, endTime)) {
                        has_no_more = false;
                        return true;
                    }
                } while (true);
            }
        }

        @Override
        public Event current() {
            synchronized (this) {
                if (currentNode == null || has_no_more)
                    throw new IllegalStateException("Method should be accessed only after moveNext() == true");

                return currentNode.event;
            }
        }

        @Override
        public void remove() {
            synchronized (this) {
                if (currentNode == null || has_no_more)
                    throw new IllegalStateException("Method should be accessed only after moveNext() == true");

                nodeControl.remove(currentNode);
                has_no_more = true;
            }
        }

        @Override
        public void close() {
            synchronized (this) {
                currentNode = null;
                nodeControl = null;
            }
        }
    }

    class EventNode {
        final Event event;
        EventNode prev;
        EventNode next;

        EventNode(Event evt){
            this.event = evt;
        }

        boolean belongs(long start, long end){
            long timestamp = event.timestamp();
            return start <= timestamp && timestamp < end;
        }
    }

    class NodeControl {
        EventNode first;
        EventNode last;
        long length;

        synchronized void insert(EventNode newEvent){
            if (first == null) first = newEvent;

            EventNode oldLast = last;
            if(oldLast != null) oldLast.next = newEvent;

            newEvent.prev = oldLast;
            last = newEvent;

            length++;
        }

        synchronized void remove(EventNode event){
            EventNode nextEvent = event.next;
            EventNode prevEvent = event.prev;

            if(event.equals(first)){
                first = nextEvent;
                first.prev = null;
            }
            if(event.equals(last)){
                last = prevEvent;
                last.next = null;
            }

            if(prevEvent != null) prevEvent.next = nextEvent;
            if(nextEvent != null) nextEvent.prev = prevEvent;

            length--;
        }
    }

    private ConcurrentHashMap<String, NodeControl> events = new ConcurrentHashMap<>();

    @Override
    public void insert(Event event) {
        String event_type = event.type();
        if(!events.containsKey(event_type))
            events.putIfAbsent(event_type, new NodeControl()); // putIfAbsent to avoid race condition

        EventNode newEvent = new EventNode(event);
        events.get(event_type).insert(newEvent);
    }

    @Override
    public void removeAll(String type) {
        events.replace(type, new NodeControl());
    }

    @Override
    public EventIterator query(String type, long startTime, long endTime) {
        // TODO: implement lazy get?
        final NodeControl control = events.get(type);
        return new EventIteratorImpl(control, startTime, endTime);
    }
}
