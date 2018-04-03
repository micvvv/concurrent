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
    
    public static class Element {

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

    private final int BUFFER_CAPACITY = 500;
    private SlidingBoundedBuffer<Element> sbb = new SlidingBoundedBuffer<Element>(BUFFER_CAPACITY);
    
    private final int numberOfPutThreads = 500;
    private final int numberOfIterations = 10000;
    
    private final int THREAD_POOL_CAPACITY = numberOfPutThreads + 1; // + 1 reading thread
    private ExecutorService executorService = Executors.newFixedThreadPool(THREAD_POOL_CAPACITY);

    private Map<String, Integer> perThreadCounters = new ConcurrentHashMap<String, Integer>(numberOfPutThreads);

    private Map<String, Integer> perThreadCountersForRead = new ConcurrentHashMap<String, Integer>(numberOfPutThreads);
    
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
    
    protected void putToBuffer() {
        String threadName = Thread.currentThread().getName();
        int newSeqNumber = getIncrementWriteThreadCounter(threadName);
        
        // here newSeqNumber is appended to threadName just to mimic some unique (distinct) content 
        Element newElement = new Element(newSeqNumber, threadName, threadName + " : " + newSeqNumber);
        sbb.put(newElement);
        
        // this affects performance - use only when debugging
        ///log.trace("added: {}", newElement);
    }
    
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
    public void testConcurrency() {

        Runnable putWorker = () -> IntStream
          .rangeClosed(1, numberOfIterations)
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
                        readElement = sbb.get();
                        
                        if (null != readElement) {
                            numberOfReadElements++;
                            
                            // this affects performance - use only when debugging
                            ///log.trace("read: {}", readElement);
                            
                            validateReadElementOrder(readElement);
                        } else {//should never happen
                            log.debug("'null' element encountered!");
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
        for (int i = 0; i < numberOfPutThreads; i++) {
            executorService.execute(putWorker);
        }
        
        try {
            Thread.sleep(10);
        } catch (Exception e) {
            e.printStackTrace();
        } 
        
        // only one reading thread in this scenario:
        executorService.execute(readWorker);
        
        try {
            executorService.awaitTermination(10, TimeUnit.SECONDS);
            executorService.shutdownNow();
            
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
