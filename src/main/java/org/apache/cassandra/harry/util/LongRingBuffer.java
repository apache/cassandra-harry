package org.apache.cassandra.harry.util;

import java.util.NoSuchElementException;

/**
 * A fixed-capacity FIFO ring buffer for primitive longs.
 *
 * Offer adds to the tail; poll removes from the head.
 * Throws {@link IllegalStateException} on offer-when-full
 * and {@link NoSuchElementException} on poll/peek-when-empty.
 */
public class LongRingBuffer
{
    private final long[] arr;
    private final int capacity;
    private int head = 0;   // index of next element to poll
    private int tail = 0;   // index where next element will be written
    private int size = 0;

    public LongRingBuffer(int capacity)
    {
        if (capacity <= 0)
            throw new IllegalArgumentException("Capacity must be positive, got: " + capacity);
        this.capacity = capacity;
        this.arr = new long[capacity];
    }

    /** Adds {@code v} to the tail of the buffer. Throws if full. */
    public void offer(long v)
    {
        if (isFull())
            throw new IllegalStateException("Buffer is full (capacity=" + capacity + ")");
        arr[tail] = v;
        tail = (tail + 1) % capacity;
        size++;
    }

    /** Removes and returns the head element. Throws if empty. */
    public long poll()
    {
        if (isEmpty())
            throw new NoSuchElementException("Buffer is empty");
        long v = arr[head];
        head = (head + 1) % capacity;
        size--;
        return v;
    }

    /** Returns the head element without removing it. Throws if empty. */
    public long peek()
    {
        if (isEmpty())
            throw new NoSuchElementException("Buffer is empty");
        return arr[head];
    }

    public int size()     { return size; }
    public int capacity() { return capacity; }
    public boolean isEmpty()  { return size == 0; }
    public boolean isFull()   { return size == capacity; }
}
