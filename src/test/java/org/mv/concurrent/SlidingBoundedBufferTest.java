package org.mv.concurrent;

import static org.junit.Assert.assertEquals;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Unit test for {@link SlidingBoundedBuffer}.
 * 
 * @author micvvv@gmail.com
 */
public class SlidingBoundedBufferTest {
    
    private static final Logger log = LoggerFactory.getLogger(SlidingBoundedBufferTest.class.getName());
    
    /** Sample buffer element class */
    public static class Element {

        /** sequental number (! own sequence is used for each writer thread) */
        private int seqNumber;
        private String creatorThreadName;
        private String content;
        
        public Element(int seqNumber, String creatorThreadName, String content) {
            this.seqNumber = seqNumber;
            this.creatorThreadName = creatorThreadName;
            this.content = content;
        }

        public int getSeqNumber() {
            return seqNumber;
        }

        public String getCreatorThreadName() {
            return creatorThreadName;
        }
        
        public String getContent() {
            return content;
        }
        
        @Override
        public String toString() {
            return "Element [seqNumber=" + seqNumber + ", content=" + content + "]";
        }
    }

    private final int BUFFER_CAPACITY = 5000;
    private SlidingBoundedBuffer<Element> sbb = new SlidingBoundedBuffer<Element>(BUFFER_CAPACITY);
    
    /** The number of threads putting elements to the buffer */
    private final int NUMBER_OF_WRITERS = 500;
    /** The number of elements for each of the writing threads to put to the buffer */
    private final int NUMBER_OF_ITERATIONS = 100000;
    
    /** This limits maximun read blocking time (milliseconds) */
    private final long MAX_READ_BLOCKING_TIME = 5000;
    
    private final int THREAD_POOL_CAPACITY = NUMBER_OF_WRITERS + 1; // + 1 reading thread
    private ExecutorService executorService = Executors.newFixedThreadPool(THREAD_POOL_CAPACITY);
    
    /** This limits the time of running the test (in seconds) - after this elapses all the threads to be shut down
     * ! Increase this if using bigger number of threads and iterations or a weaker machine ! */
    private final long AWAIT_TEST_TERMINATION_TIME = 20; 

    private Map<String, Integer> perThreadCounters = new ConcurrentHashMap<String, Integer>(NUMBER_OF_WRITERS);

    private Map<String, Integer> perThreadCountersForRead = new ConcurrentHashMap<String, Integer>(NUMBER_OF_WRITERS);
    
    private Integer getIncrementWriteThreadCounter(String threadName) {
        
        Integer counter = perThreadCounters.get(threadName);
        
        // initialize
        if (null == counter) {
            counter = Integer.valueOf(0);
        }
        
        counter = counter + 1;
        perThreadCounters.put(threadName, counter);
        
        return counter;
    }

    private Integer getUpdateReadThreadCounter(String threadName, int newCounterValue) {
        
        Integer counter = perThreadCountersForRead.get(threadName);
        
        if (null == counter) {// initialize
            counter = Integer.valueOf(0);
        }
        
        perThreadCountersForRead.put(threadName, newCounterValue);
        
        return counter;
    }
    
    private void putToBuffer() {
        String threadName = Thread.currentThread().getName();
        int newSeqNumber = getIncrementWriteThreadCounter(threadName);
        
        // here newSeqNumber is appended to threadName just to mimic some unique (distinct) content 
        Element newElement = new Element(newSeqNumber, threadName, threadName + " : " + newSeqNumber);
        
        try {
            sbb.put(newElement);
        } catch (/*Interrupted*/Exception e) {
            log.debug("Writing thread got interrupted");
        }        
        
        // this affects performance - use only when debugging
        ///log.trace("added: {}", newElement);
    }
    
    /** The order should be preserved - not 0 indicates bugs */
    private int outOfOrderCount = 0;
    
    private void validateReadElementOrder(Element elementToValidate) {
        int currentSeqNumber = elementToValidate.getSeqNumber();
        int previousSeqNumber = getUpdateReadThreadCounter(elementToValidate.getCreatorThreadName(), currentSeqNumber);
        
        if (!(currentSeqNumber > previousSeqNumber)) {
            outOfOrderCount++;
            log.debug("'Out of order' element detected: previousSeqNumber={}, currentSeqNumber={}", new Object[] {previousSeqNumber, currentSeqNumber});
        }
    }
    
    @Test
    /** 
     * Test for multiple writers and a single reader. The passing criteria is preserved order of 
     * elements (for each one of the writing threads) when reading from the buffer.
     */
    public void testConcurrency() {

        Runnable putWorker = () -> IntStream
          .rangeClosed(1, NUMBER_OF_ITERATIONS)
          .forEach(i -> putToBuffer());
         
        // reading in parallel (with one thread only):
        Runnable readWorker = new Runnable() {
            
            @Override
            public void run() {

                Element readElement = null;
                int numberOfReadElements = 0;
                long t0 = System.nanoTime();
                
                try {
                    do {
                        readElement = sbb.get(MAX_READ_BLOCKING_TIME);
                        
                        if (null != readElement) {
                            numberOfReadElements++;
                            
                            // this affects performance - use only when debugging
                            ///log.trace("read: {}", readElement);
                            
                            validateReadElementOrder(readElement);
                        }
                    } while (null != readElement);
                
                } catch (InterruptedException e) {
                    log.debug("Reading thread got interrupted");
                }
              
                long averageElementReadTime = (System.nanoTime() - t0) / numberOfReadElements / 1000;
                
                log.info("Number of read elements: {}", numberOfReadElements);
                if (0 != numberOfReadElements) {
                    log.info("Average element read time: {} microsec", averageElementReadTime);
                }
                
                if (outOfOrderCount > 0) {
                    log.error("Test has failed: 'out of order' reads count = {}", outOfOrderCount);
                }
                
                assertEquals("'Out of order' elements count", 0, outOfOrderCount);
            }
        };
        
        // a number of writers
        for (int i = 0; i < NUMBER_OF_WRITERS; i++) {
            executorService.execute(putWorker);
        }
        
        // only one reading thread in this scenario:
        executorService.execute(readWorker);
        
        try {
            executorService.awaitTermination(AWAIT_TEST_TERMINATION_TIME, TimeUnit.SECONDS);
            executorService.shutdownNow();
            
        } catch (InterruptedException e) {
            log.info("Awaiting termination interrupted", e);
        }
    }

}
