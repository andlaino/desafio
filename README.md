
A simple implementation of a **doubly linked list**, access to inner fields instead of method calls and less code (compared to the Java Concurrent Framework) was used for performance gain on big lists.

To ensure thread-safe I've used synchronized blocks and the *java.util.concurrent.ConcurrentHashMap* to map different kinds of events.

There are two test classes:

 - **net.intelie.challenges.EventStoreImplTest** which makes sure the implementation is working for general purposes.
 - **net.intelie.challenges.EventStoreImplConcurrencyTest** which uses lots of threads to test concurrent insertion and queries.

Note that due to time, not all the possible concurrent test coverage was done.