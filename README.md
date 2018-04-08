# concurrent

Java multi-threading exercises

ver. 0.0.1

SlidingBoundedBuffer

(thread-safe circular buffer based on an object array)

 - FIFO (the order is preserved)
 - elements number is limited
 - writing doesn't get blocked when the maximum number of elements is reached (instead the oldest elements are dropped from the 'head' of the buffer)
 
 possible improvements:
 
 - enforce priority of reading/removing elements from the buffer when it's close to being full (when waiting on the lock reading thread needs to receive notification before any writing threads)
 

TODO ideas

SlidingBoundedLinkedBuffer

- the same basic idea as SlidingBoundedBuffer (non-blocking writes, removing oldest elements,.. etc.)
- add pluggable weight evaluation for members, e.g. memory footprint or realtime deadline, and use that to constrict the buffer size
- add pluggable strategy for removal at overflow. E.g. while keeping the order of execution untouched, first removing those elements that would be the heaviest to process and thus most likely to ease the congestion
 
