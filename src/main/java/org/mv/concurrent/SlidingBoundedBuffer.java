package org.mv.concurrent;

import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A minimalistic thread-safe circular buffer based on an array:
 * 
 * - FIFO (the order is preserved)
 * - elements count is limited
 * - writing doesn't get blocked when the maximum is reached but instead the oldest elements
 *   are dropped from the "head" of the buffer
 * 
 * Usage scenario: multiple threads putting new elements (messages) into the buffer at the same time when 
 * one (or more) threads is reading 
 * 
 * @author micvvv@gmail.com
 *
 * @param <T> the class of contained elements
 */
public class SlidingBoundedBuffer<T> {

    private final T[] elements;
    
    private int currentCount;

    private int headIndex;
    private int tailIndex;

    private final ReentrantLock lock = new ReentrantLock();
    final Condition notEmpty = lock.newCondition();
    
    public SlidingBoundedBuffer() {
        this(64);
    }

    @SuppressWarnings("unchecked")
    public SlidingBoundedBuffer(int maxSize) {

        if (maxSize <= 0) {
            throw new IllegalArgumentException("The size should be greater than 0");
        }

        elements = (T[]) new Object[maxSize];
    }
    
    public int getBufferSize() {
        return elements.length;
    }  
    
    /**
     * Adds a new element to the tail of the buffer. 
     * This doesn't get blocked even when the buffer is full. In this case the oldest element gets dropped
     * from the head of the buffer.
     * @param newElement the element to add
     */
    public void put(T newElement) {
        
        if (null == newElement) {
            throw new IllegalArgumentException("'null' elements are not allowed");
        }
        
        lock.lock();
        try {
            elements[tailIndex] = newElement;
            tailIndex = (tailIndex + 1) % elements.length;
            
            // the oldest element just got overriden
            if (currentCount == elements.length) {
                headIndex = (headIndex + 1) % elements.length;
            } else {
                currentCount++;
            }   
            
            notEmpty.signal();
            
        } finally {
            lock.unlock();
        }
    }    
    
    /**
     * Returns the oldest element and removes it from the buffer.
     * This call gets blocked when the buffer is empty waiting for a new element to appear in the buffer.
     * @return the oldest element (from the head of the buffer)
     * @throws InterruptedException when waiting for a new element gets interrupted
     */
    public T get() throws InterruptedException {
        
        lock.lock();
        T element = take();
        
        try {
            if (null == element) {
                //System.out.println("waiting...");
                notEmpty.await();
                
                element = take();
            }
            
            return element;
            
        } finally {
            lock.unlock();
        }
    }  
    
    private T take() {
        if (currentCount == 0) {
            return null;
        }
        
        T element = elements[headIndex];
        elements[headIndex] = null;
        
        headIndex = (headIndex + 1) % elements.length;
        currentCount--;
        
        return element;
    }    
}
