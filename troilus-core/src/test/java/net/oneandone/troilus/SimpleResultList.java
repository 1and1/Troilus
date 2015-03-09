/*
 * Copyright 1&1 Internet AG, https://github.com/1and1/
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.oneandone.troilus;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.Instant;
import java.util.Date;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.UUID;

import net.oneandone.troilus.java7.FetchingIterator;

import org.testng.collections.Lists;

import com.datastax.driver.core.ExecutionInfo;
import com.datastax.driver.core.TupleValue;
import com.datastax.driver.core.UDTValue;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.AbstractFuture;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;



public class SimpleResultList implements net.oneandone.troilus.java7.ResultList<Record> {
    private final long elements;
    private final int fetchDelayMillis;
    
    public SimpleResultList(long elements, int fetchDelayMillis) {
        this.elements = elements;
        this.fetchDelayMillis = fetchDelayMillis;
    }

    public static ResultListPublisher<Record> newResultListPublisher(long elements) {
        return newResultListPublisher(elements, 0);
    }

    public static ResultListPublisher<Record> newResultListPublisher(long elements, int fetchDelayMillis) {
        return new ResultListPublisher<Record>(Futures.immediateFuture(new SimpleResultList(elements, fetchDelayMillis)));
    }
    
    @Override
    public boolean wasApplied() {
        return false;
    }
    
    @Override
    public ImmutableList<ExecutionInfo> getAllExecutionInfo() {
        return null;
    }

    @Override
    public ExecutionInfo getExecutionInfo() {
        return null;
    }
    
    @Override
    public FetchingIterator<Record> iterator() {
        return new FetchingIteratorImpl(elements, fetchDelayMillis);
    }
    

    private static final class FetchingIteratorImpl implements FetchingIterator<Record> {
        private List<Chunk> remainingChunks = Lists.newArrayList();
        private Chunk currentChunk;
        private final int fetchDelayMillis;
        
        public FetchingIteratorImpl(long elements, int fetchDelayMillis) {
            this.fetchDelayMillis = fetchDelayMillis;
        
            int id = 'A';
            do {
                
                Chunk chunk;
                if (elements > 500) {
                    chunk = new Chunk((char) id, 500);
                    elements = elements - 500;

                } else if (elements > 50) {
                    chunk = new Chunk((char) id, 50);
                    elements = elements - 50;
                    
                } else if (elements > 5) {
                    chunk = new Chunk((char) id, 5);
                    elements = elements - 5;
                    
                } else {
                    chunk = new Chunk((char) id, elements);
                    elements = 0;
                }

                System.out.println("add chunk " + chunk);
                remainingChunks.add(chunk);

                id++;
            } while (elements > 0);
        }
        
        
        
        @Override
        public int getAvailableWithoutFetching() {
            if (currentChunk == null) {
                return 0;
            } else {
                return (int) currentChunk.getAvailable();
            }
        }
        
        @Override
        public ListenableFuture<Void> fetchMoreResultsAsync() {

            return new AbstractFuture<Void>() {
                
                {
                    new Thread() {
                        
                        public void run() {
                            for (int i = 0; i< 5; i++) {
                                try {
                                    Thread.sleep(fetchDelayMillis);
                                } catch (InterruptedException ignore) { }
                                System.out.print("z");
                            }

                            loadNextChunk();
                            set(null);
                        };
                        
                    }.start();
                    
                }
            };
        }

        private void loadNextChunk() {
            System.out.println("got next chunk");
            if (remainingChunks.size() > 0) {
                currentChunk = remainingChunks.remove(0);
            } else {
                currentChunk = null;
            }
        }
        
        @Override
        public boolean isFullyFetched() {
            return (currentChunk == null) && remainingChunks.isEmpty();
        }
        
        @Override
        public boolean hasNext() {
            return currentChunk.hasNext() || (remainingChunks.size() > 0);
        }
        
        @Override
        public Record next() {
            
            if (currentChunk == null) {
                if (remainingChunks.isEmpty()) {
                    throw new NoSuchElementException();
                } else {
                    ListenableFutures.getUninterruptibly(fetchMoreResultsAsync());
                }
            }
            
            return currentChunk.next(); 
        }
        
        
        private static final class Chunk {
            private char id;
            private final long elements;
            private int pos = 0;
            
            
            public Chunk(char id, long elements) {
                this.id = id;
                this.elements = elements;
            }
            
            public long getAvailable() {
                return (elements - pos);
            }
            
            public boolean hasNext() {
                return getAvailable() > 0;
            }
            
            
            @Override
            public String toString() {
                return id + " size=" + elements;
            }
            
            public Record next() {
                if (pos == elements) {
                    throw new NoSuchElementException();
                }
                    
                pos++;
                System.out.print(id);
                return new Record() {
                    
                    @Override
                    public boolean wasApplied() {
                        // TODO Auto-generated method stub
                        return false;
                    }
                    
                    @Override
                    public ExecutionInfo getExecutionInfo() {
                        // TODO Auto-generated method stub
                        return null;
                    }
                    
                    @Override
                    public ImmutableList<ExecutionInfo> getAllExecutionInfo() {
                        // TODO Auto-generated method stub
                        return null;
                    }
                    
                    @Override
                    public boolean isNull(String name) {
                        // TODO Auto-generated method stub
                        return false;
                    }
                    
                    @Override
                    public Long getWritetime(String name) {
                        // TODO Auto-generated method stub
                        return null;
                    }
                    
                    @Override
                    public BigInteger getVarint(String name) {
                        // TODO Auto-generated method stub
                        return null;
                    }
                    
                    @Override
                    public <T> T getValue(String name, Class<T> type) {
                        // TODO Auto-generated method stub
                        return null;
                    }
                    
                    @Override
                    public <T> T getValue(ColumnName<T> name) {
                        // TODO Auto-generated method stub
                        return null;
                    }
                    
                    @Override
                    public UUID getUUID(String name) {
                        // TODO Auto-generated method stub
                        return null;
                    }
                    
                    @Override
                    public UDTValue getUDTValue(String name) {
                        // TODO Auto-generated method stub
                        return null;
                    }
                    
                    @Override
                    public TupleValue getTupleValue(String name) {
                        // TODO Auto-generated method stub
                        return null;
                    }
                    
                    @Override
                    public Duration getTtl(String name) {
                        // TODO Auto-generated method stub
                        return null;
                    }
                    
                    @Override
                    public String getString(String name) {
                        // TODO Auto-generated method stub
                        return null;
                    }
                    
                    @Override
                    public <T> ImmutableSet<T> getSet(String name, Class<T> elementsClass) {
                        // TODO Auto-generated method stub
                        return null;
                    }
                    
                    @Override
                    public <K, V> ImmutableMap<K, V> getMap(String name, Class<K> keysClass,
                            Class<V> valuesClass) {
                        // TODO Auto-generated method stub
                        return null;
                    }
                    
                    @Override
                    public long getLong(String name) {
                        // TODO Auto-generated method stub
                        return 0;
                    }
                    
                    @Override
                    public <T> ImmutableList<T> getList(String name, Class<T> elementsClass) {
                        // TODO Auto-generated method stub
                        return null;
                    }
                    
                    @Override
                    public int getInt(String name) {
                        // TODO Auto-generated method stub
                        return 0;
                    }
                    
                    @Override
                    public Instant getInstant(String name) {
                        // TODO Auto-generated method stub
                        return null;
                    }
                    
                    @Override
                    public InetAddress getInet(String name) {
                        // TODO Auto-generated method stub
                        return null;
                    }
                    
                    @Override
                    public float getFloat(String name) {
                        // TODO Auto-generated method stub
                        return 0;
                    }
                    
                    @Override
                    public <T extends Enum<T>> T getEnum(String name, Class<T> enumType) {
                        // TODO Auto-generated method stub
                        return null;
                    }
                    
                    @Override
                    public BigDecimal getDecimal(String name) {
                        // TODO Auto-generated method stub
                        return null;
                    }
                    
                    @Override
                    public Date getDate(String name) {
                        // TODO Auto-generated method stub
                        return null;
                    }
                    
                    @Override
                    public ByteBuffer getBytesUnsafe(String name) {
                        // TODO Auto-generated method stub
                        return null;
                    }
                    
                    @Override
                    public ByteBuffer getBytes(String name) {
                        // TODO Auto-generated method stub
                        return null;
                    }
                    
                    @Override
                    public boolean getBool(String name) {
                        // TODO Auto-generated method stub
                        return false;
                    }
                };
            }   
        }
    }
}
