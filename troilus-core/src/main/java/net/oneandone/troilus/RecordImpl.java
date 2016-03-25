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
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.ColumnDefinitions.Definition;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ExecutionInfo;
import com.datastax.driver.core.LocalDate;
import com.datastax.driver.core.QueryTrace.Event;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.TupleValue;
import com.datastax.driver.core.TypeCodec;
import com.datastax.driver.core.UDTValue;
import com.google.common.base.MoreObjects;
import com.google.common.base.MoreObjects.ToStringHelper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.reflect.TypeToken;

 
/**
 * The record implementation
 */
class RecordImpl implements Record {
    private static final Logger LOG = LoggerFactory.getLogger(RecordImpl.class);
    private final Tablename tablename;
    private final Context ctx;
    private final Result result;
    private final Row row;
    
    /**
     * @param ctx     the context
     * @param result  the result
     * @param row     the underlying row
     */
    RecordImpl(final Context ctx, final ReadQueryData queryData, final Result result, final Row row) {
        this.ctx = ctx;
        this.result = result;
        this.row = row;
        this.tablename = queryData.getTablename();
       
        paranoiaCheck(ctx, this, queryData);
    }

    
    private static void paranoiaCheck(final Context ctx, final Record record, final ReadQueryData data) {
        
        for (final Entry<String, ImmutableList<Object>> entry : data.getKeys().entrySet()) {
            if (record.isNull(entry.getKey())) {
                // response does not include key
                return;
            }
            
            final ByteBuffer responseKeyValue = record.getBytesUnsafe(entry.getKey());
            
            // check if response key matches with any of the request keys
            for (Object value : entry.getValue()) {
                final UDTValueMapper udtValueMapper = ctx.getUDTValueMapper();
            	if (value == null) continue;
            	try {
            	    final ByteBuffer requestKeyValue = udtValueMapper.serialize(value);
                	if (requestKeyValue.compareTo(responseKeyValue) == 0) {
                        return;
                    }
            	} catch(Exception e) {
            		LOG.warn("Cassandra 3.0 serialization failed to serialize object: " + value, e);
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
    public boolean isNull(final String name) {
        return row.isNull(name);
    }

    @Override
    public byte getByte(String name) {
        return row.getByte(name);
    }
    
    @Override
    public long getLong(final String name) {
        return row.getLong(name);
    }
    
    @Override
    public String getString(final String name) {
        return row.getString(name);
    }
    
    @Override
    public long getTime(final String name) {
    	return row.getTime(name);
    }
    
    @Override
    public boolean getBool(final String name) {
        return row.getBool(name);
    }
    
    @Override
    public ByteBuffer getBytes(final String name) {
        return row.getBytes(name);
    }

    @Override
    public ByteBuffer getBytesUnsafe(final String name) {
        return row.getBytesUnsafe(name);
    }

    @Override
    public float getFloat(final String name) {
        return row.getFloat(name);
    }
    
    @Override
    public double getDouble(final String name) {
        return row.getDouble(name);
    }

    @Override
    public Date getTimestamp(String name) {
        return row.getTimestamp(name);
    }
    
    @Override
    public LocalDate getDate(final String name) {
    	return row.getDate(name);
    }

    @Override
    public BigDecimal getDecimal(final String name) {
        return row.getDecimal(name);
    }

    @Override
    public int getInt(final String name) {
        return row.getInt(name);
    }

    @Override
    public InetAddress getInet(final String name) {
        return row.getInet(name);
    }

    @Override
    public BigInteger getVarint(final String name) {
        return row.getVarint(name);
    }
  
    @Override
    public short getShort(String name) {
        return row.getShort(name);
    }
    
    @Override
    public UUID getUUID(final String name) {
        return row.getUUID(name);
    }
    
    @Override
    public TupleValue getTupleValue(final String name) {
        return row.getTupleValue(name);
    }

    @Override
    public UDTValue getUDTValue(final String name) {
        return row.getUDTValue(name);
    }
        
    @Override
    public <T extends Enum<T>> T getEnum(final String name, final Class<T> enumType) {
        return getValue(name, enumType);
    }
    
    @Override
    public <T> T get(final String name, final Class<T> targetClass) {
        return row.get(name, targetClass);
    }
    
    @Override
    public <T> T get(final String name, final TypeToken<T> targetType) {
        return row.get(name, targetType);
    }
    
    @Override
    public <T> T get(final String name, final TypeCodec<T> codec) {
        return row.get(name, codec);
    }
    
    @Override
    public <T> T getValue(final ColumnName<T> name) {
        return name.read(this);
    }
    
    @Override
    public Object getObject(String name) {
        // TODO Auto-generated method stub
        return null;
    }
    
    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Override
    public <T> T getValue(final String name, final Class<T> elementsClass) {
        final DataType datatype = getColumnDefinitions().getType(name);
        
        if (datatype != null) {
            
            // build-in
            if (UDTValueMapper.isBuildInType(datatype)) {
         
                final ByteBuffer byteBuffer = getBytesUnsafe(name); 
                Object obj;
                if (byteBuffer == null) {
                    obj = null;
                } else  {
                	obj = ctx.getUDTValueMapper().deserialize(datatype, byteBuffer);
                }
            
                // enum
                if ((obj != null) && DataTypes.isTextDataType(datatype) && Enum.class.isAssignableFrom(elementsClass)) {
                    return (T) Enum.valueOf((Class<Enum>) elementsClass, obj.toString());
                }
                
                // bytebuffer (byte[])
                if (datatype.equals(DataType.blob()) && byte[].class.isAssignableFrom(elementsClass)) {
                    if (obj == null) {
                        return null;
                    } else {
                        final ByteBuffer bb = (ByteBuffer) obj;
                        byte[] bytes = new byte[bb.remaining()];
                        bb.get(bytes, 0, bytes.length);
                        return (T) bytes;
                    }
                }
                
                return (T) obj;
             
            // udt
            } else {
                return ctx.getUDTValueMapper().fromUdtValue(datatype, getUDTValue(name), elementsClass);
            }
        }
        
        return null;
    }
    
    
    
    @Override
    public <T> ImmutableSet<T> getSet(String name, Class<T> elementsClass) {
        if (isNull(name)) {
            return ImmutableSet.of();
        }

        final DataType datatype = ctx.getCatalog().getColumnMetadata(tablename, name).getType();
        if (UDTValueMapper.isBuildInType(datatype)) {
            return ImmutableSet.copyOf(getRow().getSet(name, elementsClass));
        } else {
            return ctx.getUDTValueMapper().fromUdtValues(datatype.getTypeArguments().get(0), ImmutableSet.copyOf(getRow().getSet(name, UDTValue.class)), elementsClass);
        }
    }
    
    @Override
    public <T> ImmutableList<T> getList(String name, Class<T> elementsClass) {
        if (isNull(name)) {
            return ImmutableList.of();
        }
        
        final DataType datatype = ctx.getCatalog().getColumnMetadata(tablename, name).getType();
        if (UDTValueMapper.isBuildInType(datatype)) {
            return ImmutableList.copyOf(getRow().getList(name, elementsClass));
        } else {
            return ctx.getUDTValueMapper().fromUdtValues(datatype.getTypeArguments().get(0), ImmutableList.copyOf(getRow().getList(name, UDTValue.class)), elementsClass);
        }
    }
    
    @Override
    public <K, V> ImmutableMap<K, V> getMap(String name, Class<K> keysClass, Class<V> valuesClass) {
        if (isNull(name)) {
            return ImmutableMap.of();
        }
        
        final DataType datatype = ctx.getCatalog().getColumnMetadata(tablename, name).getType();
        if (UDTValueMapper.isBuildInType(datatype)) {
            return ImmutableMap.copyOf(getRow().getMap(name, keysClass, valuesClass));
            
        } else {
            if (UDTValueMapper.isBuildInType(datatype.getTypeArguments().get(0))) {
                return ctx.getUDTValueMapper().fromUdtValues(datatype.getTypeArguments().get(0), datatype.getTypeArguments().get(1), ImmutableMap.copyOf(getRow().getMap(name, keysClass, UDTValue.class)), keysClass, valuesClass);

            } else if (UDTValueMapper.isBuildInType(datatype.getTypeArguments().get(1))) {
                return ctx.getUDTValueMapper().fromUdtValues(datatype.getTypeArguments().get(0), datatype.getTypeArguments().get(1), ImmutableMap.copyOf(getRow().getMap(name, UDTValue.class, valuesClass)), keysClass, valuesClass);
                
            } else {
                return ctx.getUDTValueMapper().fromUdtValues(datatype.getTypeArguments().get(0), datatype.getTypeArguments().get(1), ImmutableMap.copyOf(getRow().getMap(name, UDTValue.class, UDTValue.class)), keysClass, valuesClass);
            }
        }
    }

    @Override
    public <T> List<T> getList(String name, TypeToken<T> elementsType) {
        return row.getList(name, elementsType);
    }
    
    @Override
    public <K, V> Map<K, V> getMap(String name, TypeToken<K> keysType, TypeToken<V> valuesType) {
        return row.getMap(name, keysType,  valuesType);
    }
    
    @Override
    public <T> Set<T> getSet(String name, TypeToken<T> elementsType) {
        return row.getSet(name, elementsType);
    }
    
    @Override
    public String toString() {
        final ToStringHelper toStringHelper = MoreObjects.toStringHelper(this);
        
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
            final StringBuilder builder = new StringBuilder();
            builder.append(ctx.getUDTValueMapper().deserialize(dataType, getRow().getBytesUnsafe(name)).toString());
            
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
        	if (Collection.class.isAssignableFrom(clazz1)) {
        		throw new IllegalArgumentException("clazz1 cannot be a collection. " + 
        				                           "Call read(String name, Class<?> clazz1, Class<?> clazz2) instead.");
        	}
        	return read(name, clazz1, null);
        }
    
        
        @SuppressWarnings("unchecked")
        @Override
        public <T> Optional<T> read(String name, Class<?> clazz1, Class<?> clazz2) {
            T value = null;
        	if (List.class.isAssignableFrom(clazz1)) {
        		value = (T) record.getList(name, clazz2);
        	} else if (Set.class.isAssignableFrom(clazz1)) {
        		value = (T) record.getSet(name, clazz2);
        	} else if (clazz2 != null) {
        		// It is a Map if both clazz1 and clazz2 are set
        		// yet clazz1 is NOT a collection.  This is the only way
        		// I could figure out how to do it without changing the read API
        		// to take more parameters
        		final Class<?> keyClass = clazz1;
        		final Class<?> valuesClass = clazz2;
        		value = (T)record.getMap(name, keyClass, valuesClass);
        	} else {
        		// Its not a Set or List & clazz2 is null
        		// so, it is a single attribute
        		value = (T) record.getValue(name, clazz1);
        	}
            
        	 if (value == null) {
                 return Optional.empty();
             } else {
                 return Optional.of(value);
             }
        	
        }
    }
}