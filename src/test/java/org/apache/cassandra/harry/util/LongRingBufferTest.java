package org.apache.cassandra.harry.util;

import java.util.NoSuchElementException;

import org.junit.Test;

import static org.junit.Assert.*;

public class LongRingBufferTest
{
    // --- construction ---

    @Test(expected = IllegalArgumentException.class)
    public void zeroCapacityThrows()
    {
        new LongRingBuffer(0);
    }

    @Test(expected = IllegalArgumentException.class)
    public void negativeCapacityThrows()
    {
        new LongRingBuffer(-1);
    }

    @Test
    public void newBufferIsEmpty()
    {
        LongRingBuffer buf = new LongRingBuffer(4);
        assertTrue(buf.isEmpty());
        assertFalse(buf.isFull());
        assertEquals(0, buf.size());
        assertEquals(4, buf.capacity());
    }

    // --- single element ---

    @Test
    public void singleOfferPoll()
    {
        LongRingBuffer buf = new LongRingBuffer(1);
        buf.offer(42L);
        assertFalse(buf.isEmpty());
        assertTrue(buf.isFull());
        assertEquals(1, buf.size());
        assertEquals(42L, buf.poll());
        assertTrue(buf.isEmpty());
        assertEquals(0, buf.size());
    }

    @Test
    public void singleOfferPeekDoesNotRemove()
    {
        LongRingBuffer buf = new LongRingBuffer(4);
        buf.offer(7L);
        assertEquals(7L, buf.peek());
        assertEquals(1, buf.size());   // still there
        assertEquals(7L, buf.poll());
        assertTrue(buf.isEmpty());
    }

    // --- FIFO ordering ---

    @Test
    public void fifoOrdering()
    {
        LongRingBuffer buf = new LongRingBuffer(4);
        for (long i = 0; i < 4; i++) buf.offer(i);
        for (long i = 0; i < 4; i++) assertEquals(i, buf.poll());
    }

    // --- fill and drain ---

    @Test
    public void fillToCapacityThenDrain()
    {
        int cap = 8;
        LongRingBuffer buf = new LongRingBuffer(cap);
        for (long i = 0; i < cap; i++) buf.offer(i);
        assertTrue(buf.isFull());
        assertEquals(cap, buf.size());
        for (long i = 0; i < cap; i++) assertEquals(i, buf.poll());
        assertTrue(buf.isEmpty());
    }

    // --- wrap-around ---

    @Test
    public void wrapAroundMaintainsOrder()
    {
        LongRingBuffer buf = new LongRingBuffer(4);
        // fill completely
        buf.offer(1); buf.offer(2); buf.offer(3); buf.offer(4);
        // drain two
        assertEquals(1, buf.poll());
        assertEquals(2, buf.poll());
        // add two more (tail wraps)
        buf.offer(5); buf.offer(6);
        // drain all, expect FIFO
        assertEquals(3, buf.poll());
        assertEquals(4, buf.poll());
        assertEquals(5, buf.poll());
        assertEquals(6, buf.poll());
        assertTrue(buf.isEmpty());
    }

    @Test
    public void multipleWrapArounds()
    {
        LongRingBuffer buf = new LongRingBuffer(3);
        for (int round = 0; round < 10; round++)
        {
            long base = round * 3L;
            buf.offer(base);
            buf.offer(base + 1);
            buf.offer(base + 2);
            assertTrue(buf.isFull());
            assertEquals(base,     buf.poll());
            assertEquals(base + 1, buf.poll());
            assertEquals(base + 2, buf.poll());
            assertTrue(buf.isEmpty());
        }
    }

    @Test
    public void partialFillAndDrainRepeated()
    {
        LongRingBuffer buf = new LongRingBuffer(5);
        for (int i = 0; i < 20; i++)
        {
            buf.offer(i);
            assertEquals(i, buf.poll());
        }
        assertTrue(buf.isEmpty());
    }

    // --- boundary: capacity-1 full then refill ---

    @Test
    public void offerAfterDrainToEmpty()
    {
        LongRingBuffer buf = new LongRingBuffer(4);
        buf.offer(10); buf.offer(20);
        buf.poll(); buf.poll();
        assertTrue(buf.isEmpty());
        buf.offer(30);
        assertEquals(1, buf.size());
        assertEquals(30, buf.peek());
        assertEquals(30, buf.poll());
    }

    // --- error paths ---

    @Test(expected = IllegalStateException.class)
    public void offerWhenFullThrows()
    {
        LongRingBuffer buf = new LongRingBuffer(2);
        buf.offer(1); buf.offer(2);
        buf.offer(3); // must throw
    }

    @Test(expected = NoSuchElementException.class)
    public void pollWhenEmptyThrows()
    {
        new LongRingBuffer(4).poll();
    }

    @Test(expected = NoSuchElementException.class)
    public void peekWhenEmptyThrows()
    {
        new LongRingBuffer(4).peek();
    }

    @Test(expected = NoSuchElementException.class)
    public void pollAfterDrainThrows()
    {
        LongRingBuffer buf = new LongRingBuffer(2);
        buf.offer(1); buf.poll();
        buf.poll(); // now empty — must throw
    }

    // --- long boundary values ---

    @Test
    public void longMinMax()
    {
        LongRingBuffer buf = new LongRingBuffer(4);
        buf.offer(Long.MIN_VALUE);
        buf.offer(Long.MAX_VALUE);
        buf.offer(0L);
        buf.offer(-1L);
        assertEquals(Long.MIN_VALUE, buf.poll());
        assertEquals(Long.MAX_VALUE, buf.poll());
        assertEquals(0L,             buf.poll());
        assertEquals(-1L,            buf.poll());
    }

    // --- size tracking ---

    @Test
    public void sizeTracksCorrectlyAcrossWrap()
    {
        LongRingBuffer buf = new LongRingBuffer(4);
        assertEquals(0, buf.size());
        buf.offer(1); assertEquals(1, buf.size());
        buf.offer(2); assertEquals(2, buf.size());
        buf.poll();   assertEquals(1, buf.size());
        buf.offer(3); assertEquals(2, buf.size());
        buf.offer(4); assertEquals(3, buf.size());
        buf.offer(5); assertEquals(4, buf.size()); // full, head wrapped
        buf.poll();   assertEquals(3, buf.size());
        buf.poll();   assertEquals(2, buf.size());
        buf.poll();   assertEquals(1, buf.size());
        buf.poll();   assertEquals(0, buf.size());
    }

    // --- capacity of 1 ---

    @Test
    public void capacityOne()
    {
        LongRingBuffer buf = new LongRingBuffer(1);
        for (int i = 0; i < 5; i++)
        {
            buf.offer(i);
            assertTrue(buf.isFull());
            assertEquals(i, buf.peek());
            assertEquals(i, buf.poll());
            assertTrue(buf.isEmpty());
        }
    }
}
