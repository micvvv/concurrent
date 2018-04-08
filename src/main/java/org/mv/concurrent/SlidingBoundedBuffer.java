package org.mv.concurrent;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Thread-safe circular buffer based on an array:
 * 
 * - FIFO (the order is preserved)
 * - elements number is limited
 * - writing doesn't get blocked when the maximum number of elements is reached 
 * (instead the oldest elements are dropped from the 'head' of the buffer)
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

    /** The oldest element (to be read first) is located at this index in the array of elements */
    private int headIndex;
    /** The latest (last added) element is located at this index  in the array of elements */
    private int tailIndex;

    private final ReentrantLock lock = new ReentrantLock();
    final Condition notEmpty = lock.newCondition();
    
    public SlidingBoundedBuffer() {
        this(Integer.MAX_VALUE);
    }

    @SuppressWarnings("unchecked")
    public SlidingBoundedBuffer(int maxSize) {

        if (maxSize <= 0) {
            throw new IllegalArgumentException("The size should be greater than 0");
        }

        elements = (T[]) new Object[maxSize];
    }
    
    /**
     * Returns the capacity of this buffer.
     *
     * @return the capacity of this buffer
     */
    public int getBufferSize() {
        return elements.length;
    }

    /**
     * Returns the number of elements in this buffer.
     *
     * @return the number of elements in this buffer
     */
    public int getCurrentCount() {
        lock.lock();
        try {
            return currentCount;
        } finally {
            lock.unlock();
        }
    }
    
    /**
     * Adds a new element to the tail of the buffer. 
     * This doesn't get blocked even when the buffer is full. In this case the oldest element gets dropped
     * from the head of the buffer.
     * 
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

            if (currentCount == elements.length) {
                //the oldest element just got overriden so we need to adjust the index:
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
     * If the blocking time exceeds waitingTime 'null' is returned
     * 
     * @param waitingTime the maximum blocking time (in milliseconds)
     * @return the oldest element (from the head of the buffer)
     * @throws InterruptedException when waiting for a new element gets interrupted
     */
    public T get(long waitingTime) throws InterruptedException {
        
        lock.lock();
        T element = take();
        
        try {
            if (null == element) {
                notEmpty.await(waitingTime, TimeUnit.MILLISECONDS);
                
                element = take();
            }
            
            return element;
            
        } finally {
            lock.unlock();
        }
    }  
    
    /**
     * Returns the oldest element or null in the buffer is empty. 
     * For internal use only to avoid code duplication (not synchronized).
     * 
     * @return the oldest element or null in the buffer is empty
     */
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
