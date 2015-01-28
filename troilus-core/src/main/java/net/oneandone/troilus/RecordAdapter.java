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
import java.util.Optional;
import java.util.UUID;

import net.oneandone.troilus.ColumnName;
import net.oneandone.troilus.PropertiesSource;

import com.datastax.driver.core.ExecutionInfo;
import com.datastax.driver.core.UDTValue;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;


 

/**
 * Java8 adapter of a Record
 */
class RecordAdapter implements Record {
    
    private final net.oneandone.troilus.java7.Record record;
    

    /**
     * @param record the underylinhg record
     */
    private RecordAdapter(net.oneandone.troilus.java7.Record record) {
        this.record = record;
    }
    
    @Override
    public ExecutionInfo getExecutionInfo() {
        return record.getExecutionInfo();
    }
    
    @Override
    public ImmutableList<ExecutionInfo> getAllExecutionInfo() {
        return record.getAllExecutionInfo();
    }
    
    @Override
    public boolean wasApplied() {
        return record.wasApplied();
    }

    @Override
    public Optional<Long> getWritetime(String name) {
        return Optional.ofNullable(record.getWritetime(name));
    }
  
    @Override       
    public Optional<Duration> getTtl(String name) {
        return Optional.ofNullable(record.getTtl(name)).map(ttlSec -> Duration.ofSeconds(ttlSec));
    }
    
    @Override
    public boolean isNull(String name) {
        return record.isNull(name);
    }
     
    @Override
    public Optional<Long> getLong(String name) {
        return Optional.ofNullable(record.getLong(name));
    }
    
    @Override
    public Optional<String> getString(String name) {
        return Optional.ofNullable(record.getString(name));
    }

    @Override
    public Optional<Boolean> getBool(String name) {
        return Optional.ofNullable(record.getBool(name));
    }

    @Override
    public Optional<ByteBuffer> getBytes(String name) {
        return Optional.ofNullable(record.getBytes(name));
    }

    @Override
    public Optional<ByteBuffer> getBytesUnsafe(String name) {
        return Optional.ofNullable(record.getBytesUnsafe(name));
    }

    @Override
    public Optional<Float> getFloat(String name) {
        return Optional.ofNullable(record.getFloat(name));
    }

    @Override
    public Optional<Date> getDate(String name) {
        return Optional.ofNullable(record.getDate(name));
    }
     
    @Override
    public Optional<BigDecimal> getDecimal(String name) {
        return Optional.ofNullable(record.getDecimal(name));
    }
    
    @Override
    public Optional<Integer> getInt(String name) {
        return Optional.ofNullable(record.getInt(name));
    }
    
    @Override
    public Optional<InetAddress> getInet(String name) {
        return Optional.ofNullable(record.getInet(name));
    }
    
    @Override
    public Optional<BigInteger> getVarint(String name) {
        return Optional.ofNullable(record.getVarint(name));
    }
  
    @Override
    public Optional<UUID> getUUID(String name) {
        return Optional.ofNullable(record.getUUID(name));
    }

    @Override
    public Optional<UDTValue> getUDTValue(String name) {
        return Optional.ofNullable(record.getUDTValue(name));
    }
    
    @Override
    public Optional<Instant> getInstant(String name) {
        return getLong(name).map(millis -> Instant.ofEpochMilli(millis));
    }
  
    @Override
    public <T extends Enum<T>> Optional<T> getEnum(String name, Class<T> enumType) {
        return Optional.ofNullable(record.getEnum(name, enumType));
    }
    
    @Override
    public <T> Optional<T> getValue(ColumnName<T> name) {
        return Optional.ofNullable(record.getValue(name));
    }

    @Override
    public <T> Optional<T> getObject(String name, Class<T> elementsClass) {
        return Optional.ofNullable(record.getObject(name, elementsClass));
    }

    @Override
    public <T> Optional<ImmutableSet<T>> getSet(String name, Class<T> elementsClass) {
        return Optional.ofNullable(record.getSet(name, elementsClass));
    }

    @Override
    public <T> Optional<ImmutableList<T>> getList(String name, Class<T> elementsClass) {
        return Optional.ofNullable(record.getList(name, elementsClass));
    }
    
    @Override
    public <K, V> Optional<ImmutableMap<K, V>> getMap(String name, Class<K> keysClass, Class<V> valuesClass) {
        return Optional.ofNullable(record.getMap(name, keysClass, valuesClass));
    }
    
    @Override
    public String toString() {
        return record.toString();
    }

    /**
     * @param record  the record
     * @return the record as property source
     */
    static PropertiesSource toPropertiesSource(Record record) {
        return new PropertiesSourceAdapter(record);
    }
    
    
    private static class PropertiesSourceAdapter implements PropertiesSource {
        private final Record record;
    
        /**
         * @param record the record to adapt
         */
        public PropertiesSourceAdapter(Record record) {
            this.record = record;
        }
        
        @Override
        public <T> com.google.common.base.Optional<T> read(String name, Class<?> clazz1) {
            return read(name, clazz1, Object.class);
        }
            
        @SuppressWarnings("unchecked")
        @Override
        public <T> com.google.common.base.Optional<T> read(String name, Class<?> clazz1, Class<?> clazz2) {
            return toGuavaOptional((Optional<T>) record.getObject(name, clazz1));
        }
        
        private static <T> com.google.common.base.Optional<T> toGuavaOptional(Optional<T> optional) {
            if (optional.isPresent()) {
                return com.google.common.base.Optional.of((T) optional.get());
            } else {
                return com.google.common.base.Optional.absent();
            }
        }
    }
    
    

    /**
     * @param record  the java7-based record
     * @return the java8-based record
     */
    public static Record convertFromJava7(net.oneandone.troilus.java7.Record record) {
        return new RecordAdapter(record);
    }
    
    /**
     * @param record the java8-based record
     * @return the java7-based record
     */
    static net.oneandone.troilus.java7.Record convertToJava7(Record record) {
        
        return new net.oneandone.troilus.java7.Record() {
            
            @Override
            public boolean wasApplied() {
                return record.wasApplied();
            }
            
            @Override
            public ExecutionInfo getExecutionInfo() {
                return record.getExecutionInfo();
            }
            
            @Override
            public ImmutableList<ExecutionInfo> getAllExecutionInfo() {
                return record.getAllExecutionInfo();
            }
            
            @Override
            public boolean isNull(String name) {
                return record.isNull(name);
            }
            
            @Override
            public Long getWritetime(String name) {
                return record.getWritetime(name).orElse(null);
            }
            
            @Override
            public BigInteger getVarint(String name) {
                return record.getVarint(name).orElse(null);
            }
            
            @Override
            public <T> T getValue(ColumnName<T> name) {
                return record.getValue(name).orElse(null);
            }
            
            @Override
            public UUID getUUID(String name) {
                return record.getUUID(name).orElse(null);
            }
            
            @Override
            public UDTValue getUDTValue(String name) {
                return record.getUDTValue(name).orElse(null);           
            }
         
            @Override
            public Integer getTtl(String name) {
                return record.getTtl(name).map(duration -> (int) duration.getSeconds()).orElse(null);
            }
            
            @Override
            public String getString(String name) {
                return record.getString(name).orElse(null);
            }
            
            @Override
            public <T> ImmutableSet<T> getSet(String name, Class<T> elementsClass) {
                return record.getSet(name, elementsClass).orElse(null);
            }
            
            @Override
            public <T> T getObject(String name, Class<T> type) {
                return record.getObject(name, type).orElse(null);
            }
            
            @Override
            public <K, V> ImmutableMap<K, V> getMap(String name, Class<K> keysClass, Class<V> valuesClass) {
                return record.getMap(name, keysClass, valuesClass).orElse(null);
            }
            
            @Override
            public Long getLong(String name) {
                return record.getLong(name).orElse(null);
            }
            
            @Override
            public <T> ImmutableList<T> getList(String name, Class<T> elementsClass) {
                return record.getList(name, elementsClass).orElse(null);
            }
            
            @Override
            public Integer getInt(String name) {
                return record.getInt(name).orElse(null);
            }
            
            @Override
            public InetAddress getInet(String name) {
                return record.getInet(name).orElse(null);
            }
            
            @Override
            public Float getFloat(String name) {
                return record.getFloat(name).orElse(null);
            }
            
            @Override
            public <T extends Enum<T>> T getEnum(String name, Class<T> enumType) {
                return record.getEnum(name, enumType).orElse(null);
            }
            
            @Override
            public BigDecimal getDecimal(String name) {
                return record.getDecimal(name).orElse(null);
            }
            
            @Override
            public Date getDate(String name) {
                return record.getDate(name).orElse(null);
            }
            
            @Override
            public ByteBuffer getBytesUnsafe(String name) {
                return record.getBytesUnsafe(name).orElse(null);
            }
            
            @Override
            public ByteBuffer getBytes(String name) {
                return record.getBytes(name).orElse(null);
            }
            
            @Override
            public Boolean getBool(String name) {
                return record.getBool(name).orElse(null);
            }
        };
    }
}

    