/*
 * Opennaru, Inc. http://www.opennaru.com/
 *  
 * Copyright 2014 Opennaru, Inc. and/or its affiliates.
 * All rights reserved by Opennaru, Inc.
 * 
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
package com.opennaru.infinispan.queue;

import java.util.Set;
import java.util.concurrent.TimeUnit;

import javax.transaction.TransactionManager;

import org.apache.log4j.Logger;
import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.CacheSet;
import org.infinispan.context.Flag;


/**
 * Infinispan의 Key/Value를 이용한 Linked List형태의 Queue 자료구조 구현<br>
 *
 * Based on Open source InfiniSpanQueue<br>
 * https://github.com/nameislocus/infinispan-queue/ with new locking code to
 * make it thread-safe.<br>
 * Transaction locking must be set to PESSIMISTIC in your infinispanConfig.xml
 * to use this queue, as locking of keys is done.
 *
 * @author Junshik Jeon(service@opennaru.com, nameislocus@gmail.com) - original code
 * @author achan@informatica.com - Thread-safety, concurrency improvements
 */

public class InfinispanQueue {
	// Queue의 Tail을 가리키는 포인터
	private String QUEUE_TAIL;
	// Queue의 Head를 가리키는 포인터
	private String QUEUE_HEAD;
	// Queue의 크기
	private String QUEUE_SIZE;

	private static final Logger log = Logger.getLogger(InfinispanQueue.class);

	/**
	 * This queue is initially 3 entries in the underlying
	 * infinispan hashmap (Size, Tail, Head). More entries are added as the
	 * queue grows.  No need to fail fast for this as queue initialization
	 * is only done once when the job starts on each node.
	 *
	 * @param cache
	 * @param qName
	 * @throws SipException
	 *
	 */

	public boolean initialize(Cache<String, Object> cache, String qName) throws Exception {
		String key = UUIDFactory.generateGuid();

		AdvancedCache<String, Object> speedyCache = cache.getAdvancedCache().withFlags(Flag.FAIL_SILENTLY);

		TransactionManager tm = speedyCache.getAdvancedCache().getTransactionManager();

		QUEUE_TAIL = "QUEUE_TAIL" + "_" + qName;
		QUEUE_HEAD = "QUEUE_HEAD" + "_" + qName;
		QUEUE_SIZE = "QUEUE_SIZE" + "_" + qName;

		try {
    		tm.begin();
    		boolean lockReturn = speedyCache.getAdvancedCache().lock(QUEUE_TAIL, QUEUE_HEAD, QUEUE_SIZE);
    		log.debug("lockReturn for following keys: " + QUEUE_TAIL + ", " + QUEUE_HEAD + ", " + QUEUE_SIZE + " is: " + lockReturn);

    		if (lockReturn) {
        		if( speedyCache.get(QUEUE_TAIL) == null )
        		    speedyCache.put(QUEUE_TAIL, key);

        		if( speedyCache.get(QUEUE_HEAD) == null )
        		    speedyCache.put(QUEUE_HEAD, key);

        		if( speedyCache.get(QUEUE_SIZE) == null )
        		    speedyCache.put(QUEUE_SIZE, 0);

        		tm.commit();

        		if (log.isDebugEnabled()) {
					CacheSet<String> keyset = cache.keySet();
        		    log.debug("cache keys after queue init are: " + keyset);
        		}

                return true;
    		} else {
                try {
                    log.error("Unable to lock queue on initialization, rolling back.");
                    tm.rollback();
                } catch (Exception e1) {
                    log.error("Exception on shared queue initialization rollback:", e1);
					throw e1;
                }
    		}
		} catch (Exception e) {
            if (tm != null) {
                try {
                    log.error("Exception caught on queue initialization, rolling back.", e);
                    tm.rollback();
                } catch (Exception e1) {
                    log.error("Exception on rolling back transaction on error from queue initialization.", e1);
					throw e1;
                }
            } else {
                log.error("Exception on queue initialization on this node and TransactionManager is null.", e);
				throw e;
            }
        }

		return false;
	}

	public boolean initialize(Cache<String, Object> cache, String bo_table, int retries, int intervalSecs)
			throws Exception {
	    int retry = 0;

        //if the initialize fails, retry it
        if (!initialize(cache, bo_table)) {
            do {
                try {
                    TimeUnit.SECONDS.sleep(intervalSecs);
                } catch (InterruptedException ex) {
                    log.error("Interrupted while sleep on initializing queue", ex);
					throw new Exception("SIP-16294", ex);
                }

                if (initialize(cache, bo_table)) {//this is the first retry
                    retry++;
                    log.debug("Initialize queue successful on retry: (" +  retry+"/"+ retries + ")");
                    return true;
                } else {
                    retry++;
                    log.debug("Initialize again to avoid queue pointers collision (" +
                            retry+"/"+ retries + ")");
                }
            } while (retry < retries);

            return false;
        }

        return true;
    }

	/**
	 * Remove the queue for the base object entirely from the specified cache region
	 * @param cache the infinispan cache that contains the queue instance
	 * @throws SipException
	 *
	 */
	public void remove(Cache<String, Object> cache) throws Exception {

	    TransactionManager tm = cache.getAdvancedCache().getTransactionManager();

	    try {
	        tm.begin();
	        cache.getAdvancedCache().lock(QUEUE_TAIL, QUEUE_HEAD, QUEUE_SIZE);

	        log.trace("Removing: " + QUEUE_TAIL + " from cache");
	        cache.remove(QUEUE_TAIL);
	        log.trace("Removing: " + QUEUE_HEAD + " from cache");
	        cache.remove(QUEUE_HEAD);
	        log.trace("Removing: " + QUEUE_SIZE + " from cache");
	        cache.remove(QUEUE_SIZE);

	        tm.commit();
	    } catch (Exception e) {
	        if (tm != null) {
	            try {
	                log.error("Error on shared queue removal.  Rolling back.", e);
	                tm.rollback();
	            } catch (Exception e1) {
	                log.error("Exception when rolling back from exception on shared queue remove.", e1);
					throw e1;
	            }
	        } else {
	            log.error("Exception on shared queue remove on this node and TransactionManager is null:", e);
				throw e;
	        }
	    }
	}

	/**
	 * Queue에 Element Offer<P>
	 *
	 * Add new element to tail of queue, and setup this element as being the new tail.
	 * The tail Key is set to the future element to be added.<P>
	 *
	 * We Flag.FAIL_SILENTLY and fail fast (Flag.ZERO_LOCK_ACQUISITION_TIMEOUT)
	 * as splitting large rangerNodes can be done
	 * concurrently by multiple threads, and each split can produce multiple
	 * RangerNodes that are placed onto the queue.
	 * Pessimistic locking with no flags caused deadlocks when testing
	 * under high load.  So we fail fast with retry logic to avoid this.
	 *
	 * @param cache
	 * @param element
	 * @return true if offer was successful, false otherwise
	 * @throws SipException
	 */
	public boolean offer(Cache<String, Object> cache, InfinispanQueueElement element)
			throws Exception {
	    long begin = System.currentTimeMillis();
	    boolean canLockCache = false;

	     // 다음 Queue Element를 가리킬 포인터를 생성
	    String nextKey = UUIDFactory.generateGuid();
	    element.setNextId(nextKey);

        // 트랜잭션 사용
        long current = System.currentTimeMillis();
//        Cache<String, Object> speedyCache = cache.getAdvancedCache().withFlags(Flag.FAIL_SILENTLY,
//                Flag.ZERO_LOCK_ACQUISITION_TIMEOUT);
        long end = System.currentTimeMillis() - current;
        log.trace("offer getAdvancedCache: " + " in: " + end + " ms.");

        current = System.currentTimeMillis();
		TransactionManager tm = cache.getAdvancedCache().getTransactionManager();
        end = System.currentTimeMillis() - current;
        log.trace("offer getTransactionManager: " + " in: " + end + " ms.");

		try {
		    tm.begin();
		    current = System.currentTimeMillis();
			canLockCache = cache.getAdvancedCache().lock(QUEUE_TAIL, QUEUE_SIZE);
		    end = System.currentTimeMillis() - current;
		    log.debug("offer lock for: " + QUEUE_TAIL + ", " + QUEUE_SIZE + " is: " + canLockCache + " in: "  + end + " ms.");

		    if (canLockCache) {
    		    // Queue의 Tail에 Element 추가
    		    current = System.currentTimeMillis();
				String tailKey = (String) cache.get(QUEUE_TAIL);
				InfinispanQueueElement previousVal = (InfinispanQueueElement) cache.put(tailKey, element);
    			end = System.currentTimeMillis() - current;
    			log.trace("Put element to QUEUE_TAIL: " + tailKey + " in: " + end + " ms.");

    		    if (previousVal == null) {
    		        log.trace("Success! No Previous Value when putting to queue");
    		        current = System.currentTimeMillis();
					cache.put(QUEUE_TAIL, nextKey);
    		        end = System.currentTimeMillis() - current;
    		        log.trace("Moving QUEUE_TAIL to : " + nextKey + " in: "  + end + " ms.");

    		        current = System.currentTimeMillis();
					int newSize = (Integer) cache.get(QUEUE_SIZE) + 1;
					cache.put(QUEUE_SIZE, newSize);
    	            end = System.currentTimeMillis() - current;
    	            log.trace("Increasing QUEUE_SIZE to : " + newSize + " in: "  + end + " ms.");
    	            tm.commit();
    		        return true;
                } else {
                    log.warn("Problem! Previous Value found when putting:" + previousVal.toString());
                    tm.rollback();
                    return false;
                }
			} else { // if cant lock, rollback and swallow exception
                current = System.currentTimeMillis();
                tm.rollback();
                end = System.currentTimeMillis() - current;
                log.error("Couldn't lock - transaction offer rolled back in: " + end + " ms.");
            }
		} catch (Exception e) {
		    if (tm != null) {
		        try {
		            log.error("Exception on cached queue offer:, rolling back.", e);
		            current = System.currentTimeMillis();
		            tm.rollback();
		            end = System.currentTimeMillis() - current;
		            log.error("Transaction offer rolled back in: " + end + " ms.");
		        } catch (Exception e1) {
		            log.error("Exception on cached queue offer:", e1);
					throw new Exception("SIP-16290", e1);
		        }
		    } else {
		        log.error("Exception on cached queue offer and TransactionManager is null:", e);
				throw new Exception("SIP-16290", e);
		    }
		} finally {
		    end =  System.currentTimeMillis() - begin;
		    log.debug("Queue offer completed in: "  + end + " ms." + ", lockReturn: " + canLockCache);
		}

		return false;
	}

	/**
	 *
	 * @param cache
	 * @param element
	 * @param retries
	 * @param intervalSecs
	 * @return
	 * @throws SipException
	 */

	public boolean offer(Cache<String, Object> cache, InfinispanQueueElement element,
			int retries, int intervalSecs) throws Exception {
        int retry = 0;

        //if the offer fails, retry it
        if (!offer(cache, element)) {
            do {
                try {
                    TimeUnit.SECONDS.sleep(intervalSecs);
                } catch (InterruptedException ex) {
                    log.error("Interrupted while sleep on offering element to queue", ex);
					throw ex;
                }

                if (offer(cache, element)) {//this is the first retry
                    retry++;
                    log.debug("Offer successful on retry: (" +  retry+"/"+ retries + ")");
                    return true;
                } else {
                    retry++;
                    log.debug("Offer again to avoid queue collision (" +
                            retry+"/"+ retries + ")");
                }
            } while (retry < retries);

            return false;
        }

        return true;
	}

	/**
	 * Queue의 Element Poll <P>
	 *
	 * We Flag.FAIL_SILENTLY and fail fast (Flag.ZERO_LOCK_ACQUISITION_TIMEOUT)
     * as taking work off the shared queue can be done
     * concurrently by multiple threads, and long locks originating
     * from multiple nodes can cause deadlocks.
     *
	 *
	 * @param cache
	 * @return
	 * @throws SipException
	 */

	public InfinispanQueueElement poll(Cache<String, Object> cache) throws Exception {
	    long begin = System.currentTimeMillis();
	    boolean canLockCache = false;

		// 트랜잭션 사용
		long current = System.currentTimeMillis();
//		Cache<String, Object> speedyCache = cache.getAdvancedCache().withFlags(Flag.FAIL_SILENTLY,
//		        Flag.ZERO_LOCK_ACQUISITION_TIMEOUT);
		long end = System.currentTimeMillis() - current;
		log.trace("poll getAdvancedCache: " + " in: " + end + " ms.");

		current = System.currentTimeMillis();
		TransactionManager tm = cache.getAdvancedCache().getTransactionManager();
		end = System.currentTimeMillis() - current;
		log.trace("poll getTransactionManager: " + " in: " + end + " ms.");

		InfinispanQueueElement element = null;

		try {
		    tm.begin();
		    // Queue의 Head의 포인터의 값을 get
		    current = System.currentTimeMillis();
			canLockCache = cache.getAdvancedCache().lock(QUEUE_HEAD, QUEUE_SIZE);
            end = System.currentTimeMillis() - current;
            log.debug("poll lock for " + QUEUE_HEAD + ", " + QUEUE_SIZE + " is: " + canLockCache + " in: " + end + " ms.");

            if (canLockCache) {
                current = System.currentTimeMillis();
				String key = (String) cache.get(QUEUE_HEAD);
    			end = System.currentTimeMillis() - current;
    			log.trace("Queue head key: " + key + " in: "  + end + " ms.");
				element = (InfinispanQueueElement) cache.get(key);

    			// Queue Head 포인터값 Update & 제거
    			if( element != null ) {
    			    String nextId = element.getNextId();
    			    current = System.currentTimeMillis();
					cache.put(QUEUE_HEAD, nextId);
    			    end = System.currentTimeMillis() - current;
    				log.trace("Element retrieved, QUEUE_HEAD is now: " + nextId + " in: " + end + " ms.");

    				current = System.currentTimeMillis();
					int newSize = (Integer) cache.get(QUEUE_SIZE) - 1;
					cache.put(QUEUE_SIZE, newSize);
    				end = System.currentTimeMillis() - current;
    				log.trace("Decreasing QUEUE_SIZE to: " + newSize + " in: " + end + " ms.");

    				current = System.currentTimeMillis();
					cache.remove(key);
    				end = System.currentTimeMillis() - current;
    				log.trace("Old QUEUE_HEAD removed: " + key + " in: " + end + " ms.");
    			} else {
    			    log.trace("NULL element from QUEUE_HEAD key!! : " + key );
    			}
    			current = System.currentTimeMillis();
    		    tm.commit();
    		    end = System.currentTimeMillis() - current;
    		    log.trace("commit called in: " + end + " ms.");
            } else {
                current = System.currentTimeMillis();
                tm.rollback();
                end = System.currentTimeMillis() - current;
                log.error("Couldnt lock - transaction poll rolled back in: " + end + " ms.");
            }
		} catch (Exception e) {
		    if (tm != null) {
		        try {
		            log.error("Exception on cached queue poll:, rolling back.", e);
		            current = System.currentTimeMillis();
		            tm.rollback();
		            end = System.currentTimeMillis() - current;
		            log.error("Transaction poll rolled back in: " + end + " ms.");
		        } catch (Exception e1) {
		            log.error("Exception on cached queue poll:", e1);
					throw e1;
		        }
		    } else {
		        log.error("Exception on cached queue poll and TransactionManager is null:", e);
				throw e;
		    }
		} finally {
            end =  System.currentTimeMillis() - begin;
            log.debug("Queue poll completed in: "  + end + " ms." + ", lockReturn: " + canLockCache);
        }

		return element;
	}

	/**
	 *
	 * @param cache
	 * @param retries
	 * @param intervalSecs
	 * @param tm
	 * @param totalThreads
	 * @param cdata
	 * @param boName
	 * @return
	 * @throws Exception
	 */
	public InfinispanQueueElement poll(Cache<String, Object> cache, int retries, int intervalSecs)
			throws Exception {
	    boolean queueIsReallyEmpty = true;
	    int retry = 0;

	    InfinispanQueueElement retval = poll(cache);

	    if (retval == null) {
    	    do {
    	        try {
    	            TimeUnit.SECONDS.sleep(intervalSecs);
    	        } catch (InterruptedException ie) {
                    log.error("Interrupted while sleep on polling element from queue", ie);
					throw ie;
                }

                retval = poll(cache);
                if (retval != null) {
                    log.info("Poll on cached work queue still has work, continue processing.");
                    queueIsReallyEmpty = false;
                    retry = retries; // this resets the counter and breaks us out of the do-while loop
				} else {
					retry++;
					log.info("Poll again to check if sharedQ is really empty (" + retry + "/" + retries + ")");
                }
            } while (retry < retries);

    	    if (queueIsReallyEmpty) {
                return null;
            }
	    }

	    return retval;

	}
	/**
	 * Queue에 들어있는 데이터 출력
	 *
	 * @param cache
	 */
	public void printInfo(Cache<String, Object> cache) {
		System.out.println("========= Queue Info =========");
		System.out.println("Queue Size=" + (Integer) cache.get(QUEUE_SIZE));
		String headKey = (String) cache.get(QUEUE_HEAD);
		System.out.println("Queue Head=" + headKey);
		System.out.println("Queue Tail=" + cache.get(QUEUE_TAIL));

		InfinispanQueueElement element = new InfinispanQueueElement();
		while(true) {
			element = (InfinispanQueueElement) cache.get(headKey);
			if( element == null ) {
				break;
			}
			System.out.println("Queue> " + element);
			headKey = element.getNextId();
		}
		System.out.println("========= Queue END =========");

	}

	public void logInfo(Cache<String, Object> cache) {
        log.info("========= Queue Info =========");
        log.info("Queue Size "+ "(" + QUEUE_SIZE + ")=" + (Integer) cache.get(QUEUE_SIZE));
        String headKey = (String) cache.get(QUEUE_HEAD);
        log.info("Queue Head "+ "(" + QUEUE_HEAD + ")=" + headKey);
        log.info("Queue Tail " + "(" + QUEUE_TAIL + ")=" + cache.get(QUEUE_TAIL));

        if (log.isDebugEnabled()) {
            InfinispanQueueElement element = new InfinispanQueueElement();
            while(true) {
                element = (InfinispanQueueElement) cache.get(headKey);
                if( element == null ) {
                    break;
                }
                log.debug("Queue> " + element);
                headKey = element.getNextId();
            }
        }

        log.info("========= Queue END =========");

	}

	public int size(Cache<?, ?> cache) {
	    if (cache != null)
	        return (Integer) cache.get(QUEUE_SIZE);
	    return 0;
	}
}
