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
import java.util.Date;
import java.util.Map.Entry;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.oneandone.troilus.java7.Record;
import net.oneandone.troilus.java7.interceptor.ReadQueryData;

import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.ColumnDefinitions.Definition;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ExecutionInfo;
import com.datastax.driver.core.TupleValue;
import com.datastax.driver.core.QueryTrace.Event;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.UDTValue;
import com.google.common.base.MoreObjects;
import com.google.common.base.Optional;
import com.google.common.base.MoreObjects.ToStringHelper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

 
/**
 * The record implementation
 */
class RecordImpl implements Record {
    
    private static final Logger LOG = LoggerFactory.getLogger(RecordImpl.class);

    
    private final Context ctx;
    private final Result result;
    private final Row row;
    
    /**
     * @param ctx     the context
     * @param result  the result
     * @param row     the underlying row
     */
    RecordImpl(Context ctx, ReadQueryData queryData, Result result, Row row) {
        this.ctx = ctx;
        this.result = result;
        this.row = row;
       
        paranoiaCheck(ctx, this, queryData);
    }

    
    private static void paranoiaCheck(Context ctx, Record record, ReadQueryData data) {
        
        
        for (Entry<String, ImmutableList<Object>> entry : data.getKeys().entrySet()) {
           
            if (record.isNull(entry.getKey())) {
                // response does not include key
                return;
            }

            
            ByteBuffer responseKeyValue = record.getBytesUnsafe(entry.getKey());

            
            // check if response key matches with any of the request keys
            for (Object value : entry.getValue()) {
                ByteBuffer requestKeyValue = DataType.serializeValue(value, ctx.getDbSession().getProtocolVersion());
                
                if (requestKeyValue.compareTo(responseKeyValue) == 0) {
                    return;
                }
            }
            
            LOG.warn("Dataswap error for " + entry.getKey());
            throw new ProtocolErrorException("Dataswap error for " + entry.getKey()); 
        }
    }
    
    
    
    
    
    /**
     * @return the underlying row
     */
    Row getRow() {
        return row;
    }
    
    /**
     * @return  the columns returned in this ResultSet
     */
    ColumnDefinitions getColumnDefinitions() {
        return row.getColumnDefinitions();
    }
    
    @Override
    public ExecutionInfo getExecutionInfo() {
        return result.getExecutionInfo();
    }
    
    @Override
    public ImmutableList<ExecutionInfo> getAllExecutionInfo() {
        return result.getAllExecutionInfo();
    }
    
    @Override
    public boolean wasApplied() {
        return result.wasApplied();
    }

    @Override
    public Long getWritetime(String name) {
        try {
            return row.getLong("WRITETIME(" + name + ")");
        } catch (IllegalArgumentException iae) {
            return null;
        }
    }
    
    @Override       
    public Integer getTtl(String name) {
        try {
            return row.getInt("TTL(" + name + ")");
        } catch (IllegalArgumentException iae) {
            return null;
        }
    }
    
    @Override
    public boolean isNull(String name) {
        return row.isNull(name);
    }

    @Override
    public long getLong(String name) {
        return row.getLong(name);
    }
    
    @Override
    public String getString(String name) {
        return row.getString(name);
    }
    
    @Override
    public long getTime(String name) {
        return row.getTime(name);
    }
    
    @Override
    public boolean getBool(String name) {
        return row.getBool(name);
    }
    
    @Override
    public ByteBuffer getBytes(String name) {
        return row.getBytes(name);
    }

    @Override
    public ByteBuffer getBytesUnsafe(String name) {
        return row.getBytesUnsafe(name);
    }

    @Override
    public float getFloat(String name) {
        return row.getFloat(name);
    }

    @Override
    public Date getDate(String name) {
        return new Date(row.getDate(name).getMillisSinceEpoch());
    }

    @Override
    public BigDecimal getDecimal(String name) {
        return row.getDecimal(name);
    }

    @Override
    public int getInt(String name) {
        return row.getInt(name);
    }

    @Override
    public InetAddress getInet(String name) {
        return row.getInet(name);
    }

    @Override
    public BigInteger getVarint(String name) {
        return row.getVarint(name);
    }
  
    @Override
    public UUID getUUID(String name) {
        return row.getUUID(name);
    }
    
    @Override
    public TupleValue getTupleValue(String name) {
        return row.getTupleValue(name);
    }

    @Override
    public UDTValue getUDTValue(String name) {
        return row.getUDTValue(name);
    }
        
    @Override
    public <T extends Enum<T>> T getEnum(String name, Class<T> enumType) {
        return getValue(name, enumType);
    }
    
    @Override
    public <T> T getValue(ColumnName<T> name) {
        return name.read(this);
    }
    
    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Override
    public <T> T getValue(String name, Class<T> elementsClass) {
        DataType datatype = getColumnDefinitions().getType(name);
        
        if (datatype != null) {
            
            // build-in
            if (UDTValueMapper.isBuildInType(datatype)) {
         
                ByteBuffer byteBuffer = getBytesUnsafe(name); 
                Object obj;
                if (byteBuffer == null) {
                    obj = null;
                } else  {
                    obj = datatype.deserialize(byteBuffer, ctx.getDbSession().getProtocolVersion());
                }
            
                // enum
                if ((obj != null) && ctx.isTextDataType(datatype) && Enum.class.isAssignableFrom(elementsClass)) {
                    return (T) Enum.valueOf((Class<Enum>) elementsClass, obj.toString());
                }
                
                // bytebuffer (byte[])
                if (datatype.equals(DataType.blob()) && byte[].class.isAssignableFrom(elementsClass)) {
                    if (obj == null) {
                        return (T) new byte[0];
                    } else {
                        ByteBuffer bb = (ByteBuffer) obj;
                        byte[] bytes = new byte[bb.remaining()];
                        bb.get(bytes, 0, bytes.length);
                        return (T) bytes;
                    }
                }
                
                return (T) obj;
             
            // udt
            } else {
                return ctx.getDbSession().getUDTValueMapper().fromUdtValue(datatype, getUDTValue(name), elementsClass);
            }
        }
        
        return null;
    }
    
    
    
    @Override
    public <T> ImmutableSet<T> getSet(String name, Class<T> elementsClass) {
        if (isNull(name)) {
            return ImmutableSet.of();
        }

        DataType datatype = ctx.getDbSession().getColumnMetadata(name).getType();
        if (UDTValueMapper.isBuildInType(datatype)) {
            return ImmutableSet.copyOf(getRow().getSet(name, elementsClass));
        } else {
            return ctx.getDbSession().getUDTValueMapper().fromUdtValues(datatype.getTypeArguments().get(0), ImmutableSet.copyOf(getRow().getSet(name, UDTValue.class)), elementsClass);
        }
    }
    
    @Override
    public <T> ImmutableList<T> getList(String name, Class<T> elementsClass) {
        if (isNull(name)) {
            return ImmutableList.of();
        }
        
        DataType datatype = ctx.getDbSession().getColumnMetadata(name).getType();
        if (UDTValueMapper.isBuildInType(datatype)) {
            return ImmutableList.copyOf(getRow().getList(name, elementsClass));
        } else {
            return ctx.getDbSession().getUDTValueMapper().fromUdtValues(datatype.getTypeArguments().get(0), ImmutableList.copyOf(getRow().getList(name, UDTValue.class)), elementsClass);
        }
    }
    
    @Override
    public <K, V> ImmutableMap<K, V> getMap(String name, Class<K> keysClass, Class<V> valuesClass) {
        if (isNull(name)) {
            return ImmutableMap.of();
        }
        
        DataType datatype = ctx.getDbSession().getColumnMetadata(name).getType();
        if (UDTValueMapper.isBuildInType(datatype)) {
            return ImmutableMap.copyOf(getRow().getMap(name, keysClass, valuesClass));
            
        } else {
            if (UDTValueMapper.isBuildInType(datatype.getTypeArguments().get(0))) {
                return ctx.getDbSession().getUDTValueMapper().fromUdtValues(datatype.getTypeArguments().get(0), datatype.getTypeArguments().get(1), ImmutableMap.copyOf(getRow().getMap(name, keysClass, UDTValue.class)), keysClass, valuesClass);

            } else if (UDTValueMapper.isBuildInType(datatype.getTypeArguments().get(1))) {
                return ctx.getDbSession().getUDTValueMapper().fromUdtValues(datatype.getTypeArguments().get(0), datatype.getTypeArguments().get(1), ImmutableMap.copyOf(getRow().getMap(name, UDTValue.class, valuesClass)), keysClass, valuesClass);
                
            } else {
                return ctx.getDbSession().getUDTValueMapper().fromUdtValues(datatype.getTypeArguments().get(0), datatype.getTypeArguments().get(1), ImmutableMap.copyOf(getRow().getMap(name, UDTValue.class, UDTValue.class)), keysClass, valuesClass);
            }
        }
    }
    
    @Override
    public String toString() {
        ToStringHelper toStringHelper = MoreObjects.toStringHelper(this);
        
        for (Definition definition : getRow().getColumnDefinitions().asList()) {
            toStringHelper.add(definition.getName(), toString(definition.getName(), definition.getType()));
        }

        String s = "[" + result.getExecutionInfo().getQueriedHost() + "] "+  toStringHelper.toString();

        if (result.getExecutionInfo().getQueryTrace() != null) {
            StringBuilder sb = new StringBuilder("\n");
            for (Event event : result.getExecutionInfo().getQueryTrace().getEvents()) {
                sb.append(event.getSource() + " - " + event.getSourceElapsedMicros() + ": " + event.getDescription() +"\n");
            }
            s = s + sb.toString();
        }
        
        return s;
    }
    
    private String toString(String name, DataType dataType) {
        if (isNull(name)) {
            return "";
        } else {
            StringBuilder builder = new StringBuilder();
            builder.append(dataType.deserialize(getRow().getBytesUnsafe(name), ctx.getDbSession().getProtocolVersion()));

            return builder.toString();
        }
    }
    
 
    /**
     * @param record   the record
     * @return the record as properties source
     */
    static PropertiesSource toPropertiesSource(Record record) {
        return new PropertiesSourceAdapter(record);
    }
    

    
    private static class PropertiesSourceAdapter implements PropertiesSource {
        
        private final Record record;
        
        public PropertiesSourceAdapter(Record record) {
            this.record = record;
        }
        
        @Override
        public <T> Optional<T> read(String name, Class<?> clazz1) {
            return read(name, clazz1, Object.class);
        }
    
        
        @SuppressWarnings("unchecked")
        @Override
        public <T> Optional<T> read(String name, Class<?> clazz1, Class<?> clazz2) {
            T value = (T) record.getValue(name, clazz1);
            if (value == null) {
                return Optional.absent();
            } else {
                return Optional.of(value);
            }
        }
    }
}

    