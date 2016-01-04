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
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Date;
import java.util.UUID;

import net.oneandone.troilus.ColumnName;

import com.datastax.driver.core.ExecutionInfo;
import com.datastax.driver.core.TupleValue;
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
    public Long getWritetime(String name) {
        return record.getWritetime(name);
    }
  
    @Override       
    public Duration getTtl(String name) {
        final Integer ttlSec = record.getTtl(name);
        if (ttlSec == null) {
            return null;
        } else {
            return Duration.ofSeconds(ttlSec);
        }
    }
    
    @Override
    public boolean isNull(String name) {
        return record.isNull(name);
    }
     
    @Override
    public long getLong(String name) {
        return record.getLong(name);
    }
    
    @Override
    public String getString(String name) {
        return record.getString(name);
    }

    @Override
    public boolean getBool(String name) {
        return record.getBool(name);
    }

    @Override
    public ByteBuffer getBytes(String name) {
        return record.getBytes(name);
    }

    @Override
    public ByteBuffer getBytesUnsafe(String name) {
        return record.getBytesUnsafe(name);
    }

    @Override
    public float getFloat(String name) {
        return record.getFloat(name);
    }

    @Override
    public Instant getDate(String name) {
        return record.getDate(name).toInstant();
    }
    
    @Override
    public long getTime(String name) {
        return record.getTime(name);
    }
    
    @Override
    public LocalDateTime getLocalDateTime(String name) {
        final Instant instant = Instant.ofEpochMilli(getTime(name));
        return LocalDateTime.ofInstant(instant, ZoneOffset.UTC);
    }
     
    @Override
    public BigDecimal getDecimal(String name) {
        return record.getDecimal(name);
    }
    
    @Override
    public int getInt(String name) {
        return record.getInt(name);
    }
    
    @Override
    public InetAddress getInet(String name) {
        return record.getInet(name);
    }
    
    @Override
    public BigInteger getVarint(String name) {
        return record.getVarint(name);
    }
  
    @Override
    public UUID getUUID(String name) {
        return record.getUUID(name);
    }
    
    @Override
    public TupleValue getTupleValue(String name) {
        return record.getTupleValue(name);
    }

    @Override
    public UDTValue getUDTValue(String name) {
        return record.getUDTValue(name);
    }
    
    @Override
    public Instant getInstant(String name) {
        final Long millis = getLong(name);
       if (millis == 0) {
           return null;
       } else {
           return Instant.ofEpochMilli(millis);
       }
    }
  
    @Override
    public <T extends Enum<T>> T getEnum(String name, Class<T> enumType) {
        return record.getEnum(name, enumType);
    }
    
    @Override
    public <T> T getValue(ColumnName<T> name) {
        return record.getValue(name);
    }

    @Override
    public <T> T getValue(String name, Class<T> elementsClass) {
        return record.getValue(name, elementsClass);
    }

    @Override
    public <T> ImmutableSet<T> getSet(String name, Class<T> elementsClass) {
        return record.getSet(name, elementsClass);
    }

    @Override
    public <T> ImmutableList<T> getList(String name, Class<T> elementsClass) {
        return record.getList(name, elementsClass);
    }
    
    @Override
    public <K, V>ImmutableMap<K, V> getMap(String name, Class<K> keysClass, Class<V> valuesClass) {
        return record.getMap(name, keysClass, valuesClass);
    }
    
    @Override
    public String toString() {
        return record.toString();
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
                return record.getWritetime(name);
            }
            
            @Override
            public BigInteger getVarint(String name) {
                return record.getVarint(name);
            }
            
            @Override
            public <T> T getValue(ColumnName<T> name) {
                return record.getValue(name);
            }
            
            @Override
            public UUID getUUID(String name) {
                return record.getUUID(name);
            }
            
            @Override
            public TupleValue getTupleValue(String name) {
                return record.getTupleValue(name);
            }
            
            @Override
            public UDTValue getUDTValue(String name) {
                return record.getUDTValue(name);           
            }
         
            @Override
            public Integer getTtl(String name) {
                final Duration ttl = record.getTtl(name);
                if (ttl == null) {
                    return null;
                } else {
                    return (int) ttl.getSeconds();
                }
            }
            
            @Override
            public String getString(String name) {
                return record.getString(name);
            }
            
            @Override
            public <T> ImmutableSet<T> getSet(String name, Class<T> elementsClass) {
                return record.getSet(name, elementsClass);
            }
            
            @Override
            public <T> T getValue(String name, Class<T> type) {
                return record.getValue(name, type);
            }
            
            @Override
            public <K, V> ImmutableMap<K, V> getMap(String name, Class<K> keysClass, Class<V> valuesClass) {
                return record.getMap(name, keysClass, valuesClass);
            }
            
            @Override
            public long getLong(String name) {
                return record.getLong(name);
            }
            
            @Override
            public <T> ImmutableList<T> getList(String name, Class<T> elementsClass) {
                return record.getList(name, elementsClass);
            }
            
            @Override
            public int getInt(String name) {
                return record.getInt(name);
            }
            
            @Override
            public InetAddress getInet(String name) {
                return record.getInet(name);
            }
            
            @Override
            public float getFloat(String name) {
                return record.getFloat(name);
            }
            
            @Override
            public <T extends Enum<T>> T getEnum(String name, Class<T> enumType) {
                return record.getEnum(name, enumType);
            }
            
            @Override
            public BigDecimal getDecimal(String name) {
                return record.getDecimal(name);
            }
            
            @Override
            public long getTime(String name) {
                return record.getTime(name);
            }
            
            @Override
            public Date getDate(String name) {
                return Date.from(record.getDate(name));
            }
            
            @Override
            public ByteBuffer getBytesUnsafe(String name) {
                return record.getBytesUnsafe(name);
            }
            
            @Override
            public ByteBuffer getBytes(String name) {
                return record.getBytes(name);
            }
            
            @Override
            public boolean getBool(String name) {
                return record.getBool(name);
            }
        };
    }
}