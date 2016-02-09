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



import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;






import com.datastax.driver.core.CodecRegistry;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.TupleType;
import com.datastax.driver.core.TypeCodec;
import com.datastax.driver.core.UDTValue;
import com.datastax.driver.core.UserType;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableCollection;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;


/**
 * UDTValueMapper
 */
class UDTValueMapper {

    private final ProtocolVersion protocolVersion;
    private final BeanMapper beanMapper;
    private final MetadataCatalog catalog;
    private final CodecRegistry codecRegistry = CodecRegistry.DEFAULT_INSTANCE;
    
    UDTValueMapper(ProtocolVersion protocolVersion, MetadataCatalog catalog, BeanMapper beanMapper) {
        this.protocolVersion = protocolVersion;
        this.catalog = catalog;
        this.beanMapper = beanMapper;
    }
    
      
    static boolean isBuildInType(DataType dataType) {        
        if (dataType.isCollection()) {
            for (DataType type : dataType.getTypeArguments()) {
                if (!isBuildInType(type)) {
                    return false;
                }
            }
            return true;

        } else {
            return DataType.allPrimitiveTypes().contains(dataType) || (TupleType.class.isAssignableFrom(dataType.getClass()));
        }
    }
    
    
    
    /**
     * @param datatype   the db datatype
     * @param udtValue   the udt value
     * @param fieldtype1 the field 1 type
     * @param fieldtype2 the field 2 type
     * @param fieldname  the fieldname
     * @return the mapped value or <code>null</code>
     */
    public <T> Object fromUdtValue(DataType datatype, 
                               UDTValue udtValue,
                               Class<?> fieldtype1, 
                               Class<?> fieldtype2,
                               String fieldname) {
    	final CodecRegistry codecRegistry = getCodecRegistry();
    	
        // build-in type 
        if (isBuildInType(datatype)) {
            final TypeCodec<T> typeCodec = codecRegistry.codecFor(datatype);
            
            try {
            	if (udtValue.isNull(fieldname)) return null;
            	return typeCodec.deserialize(udtValue.getBytesUnsafe(fieldname), protocolVersion);
            } catch(IllegalArgumentException ex) {
            	return null;
            }
            
        // udt collection    
        } else if (datatype.isCollection()) {
           
            // set
        	 if (DataType.Name.SET == datatype.getName()) {
                return fromUdtValues(datatype.getTypeArguments().get(0), 
                                     ImmutableSet.copyOf(udtValue.getSet(fieldname, UDTValue.class)), 
                                     fieldtype2); 
                
            // list
        	 } else if (DataType.Name.LIST == datatype.getName()) {
                return fromUdtValues(datatype.getTypeArguments().get(0), 
                                     ImmutableList.copyOf(udtValue.getList(fieldname, UDTValue.class)),
                                     fieldtype2); 
                
            // map
            } else {
                if (isBuildInType(datatype.getTypeArguments().get(0))) {
                    return fromUdtValues(datatype.getTypeArguments().get(0), 
                                         datatype.getTypeArguments().get(1), 
                                         ImmutableMap.<Object, Object>copyOf(udtValue.getMap(fieldname, fieldtype1, UDTValue.class)), 
                                         fieldtype1, 
                                         fieldtype2);

                } else if (isBuildInType(datatype.getTypeArguments().get(1))) {
                    return fromUdtValues(datatype.getTypeArguments().get(0), 
                                         datatype.getTypeArguments().get(1), 
                                         ImmutableMap.<Object, Object>copyOf(udtValue.getMap(fieldname, UDTValue.class, fieldtype2)), 
                                         fieldtype1, 
                                         fieldtype2);
                    
                } else {
                    return fromUdtValues(datatype.getTypeArguments().get(0), 
                                         datatype.getTypeArguments().get(1), 
                                         ImmutableMap.<Object, Object>copyOf(udtValue.getMap(fieldname, UDTValue.class, UDTValue.class)),
                                         fieldtype1, 
                                         fieldtype2);
                }
            }
                        
        // udt    
        } else {
            return fromUdtValue(datatype, udtValue, fieldtype1);
        }
    }
    

    
    public <T> T fromUdtValue(final DataType datatype, final UDTValue udtValue, Class<T> type) {
        
        PropertiesSource propsSource = new PropertiesSource() {
            
            @Override
            public <E> Optional<E> read(String name, Class<?> clazz1) {
                return read(name, clazz1, Object.class);
            }
            
            @SuppressWarnings("unchecked")
            @Override
            public <E> Optional<E> read(String name, Class<?> clazz1, Class<?> clazz2) {
                return Optional.fromNullable((E) fromUdtValue(((UserType) datatype).getFieldType(name), udtValue, clazz1, clazz2, name));
            }
        };
        
        return beanMapper.fromValues(type, propsSource, ImmutableSet.<String>of());
    }

    
    public <T> ImmutableSet<T> fromUdtValues(final DataType datatype, ImmutableSet<UDTValue> udtValues, Class<T> type) {
        return ImmutableSet.copyOf(fromUdtValues(datatype, (ImmutableCollection<UDTValue>) udtValues, type));
    }

    
    public <T> ImmutableList<T> fromUdtValues(final DataType datatype, ImmutableList<UDTValue> udtValues, Class<T> type) {
        return fromUdtValues(datatype, (ImmutableCollection<UDTValue>) udtValues, type);
    }

    
    private <T> ImmutableList<T> fromUdtValues(final DataType datatype, ImmutableCollection<UDTValue> udtValues, Class<T> type) {
        List<T> elements = Lists.newArrayList();
        
        for (UDTValue elementUdtValue : udtValues) {
            final UDTValue elementUdtVal = elementUdtValue;
            
            final PropertiesSource propsSource = new PropertiesSource() {
                
                @Override
                public <E> Optional<E> read(String name, Class<?> clazz1) {
                    return read(name, clazz1, Object.class);
                }
                
                @SuppressWarnings("unchecked")
                @Override
                public <E> Optional<E>  read(String name, Class<?> clazz1, Class<?> clazz2) {
                    return Optional.fromNullable((E) fromUdtValue(((UserType) datatype).getFieldType(name), elementUdtVal, clazz1, clazz2, name));
                }
            };

            
            T element = beanMapper.fromValues(type, propsSource, ImmutableSet.<String>of());
            elements.add(element);
        }
        
        return ImmutableList.copyOf(elements);
    }
    
    
    
    @SuppressWarnings("unchecked")
    public <K, V> ImmutableMap<K, V> fromUdtValues(final DataType keyDatatype, final DataType valueDatatype, ImmutableMap<?, ?> udtValues, Class<K> keystype, Class<V> valuesType) {
        
        final Map<K, V> elements = Maps.newHashMap();

        for (Entry<?, ?> entry : udtValues.entrySet()) {
        
            K keyElement;
            if (keystype.isAssignableFrom(entry.getKey().getClass())) {
                keyElement = (K) entry.getKey(); 
                
            } else {
                final UDTValue keyUdtValue = (UDTValue) entry.getKey();
                
                final PropertiesSource propsSource = new PropertiesSource() {
                    
                    @Override
                    public <E> Optional<E> read(String name, Class<?> clazz1) {
                        return read(name, clazz1, Object.class);
                    }
                    
                    @Override
                    public <T> Optional<T> read(String name, Class<?> clazz1, Class<?> clazz2) {
                        return Optional.fromNullable((T) fromUdtValue(((UserType) keyDatatype).getFieldType(name), keyUdtValue, clazz1, clazz2, name));
                    }
                };

                keyElement = beanMapper.fromValues(keystype, propsSource, ImmutableSet.<String>of());
            }
            
            
            
            V valueElement;
            if (valuesType.isAssignableFrom(entry.getValue().getClass())) {
                valueElement = (V) entry.getValue();
                
            } else {
                final UDTValue valueUdtValue = (UDTValue) entry.getValue();

                final PropertiesSource propsSource = new PropertiesSource() {
                    
                    @Override
                    public <E> Optional<E> read(String name, Class<?> clazz1) {
                        return read(name, clazz1, Object.class);
                    }
                    
                    @Override
                    public <T> Optional<T> read(String name, Class<?> clazz1, Class<?> clazz2) {
                        return Optional.fromNullable((T) fromUdtValue(((UserType) valueDatatype).getFieldType(name), valueUdtValue, clazz1, clazz2, name));
                    }
                };
                
                valueElement = beanMapper.fromValues(valuesType, propsSource, ImmutableSet.<String>of());
            }

            elements.put(keyElement, valueElement);
        }
        
        return ImmutableMap.copyOf(elements);
    }
    
    
    @SuppressWarnings("unchecked")
    public Object toUdtValue(Tablename tablename,
                             MetadataCatalog catalog, 
                             DataType datatype, 
                             Object value) {
        
        // build-in type (will not be converted)
        if (isBuildInType(datatype)) {
            return value;
            
        // udt collection
        } else if (datatype.isCollection()) {
           
           // set
        	if (DataType.Name.SET == datatype.getName()) {
        	   final DataType elementDataType = datatype.getTypeArguments().get(0);
               
               final Set<Object> udt = Sets.newHashSet();
               if (value != null) {
                   for (Object element : (Set<Object>) value) {
                       udt.add(toUdtValue(tablename, catalog, elementDataType, element));
                   }
               }
               
               return ImmutableSet.copyOf(udt);
               
           // list 
        	 } else if (DataType.Name.LIST == datatype.getName()) {    
        	     final DataType elementDataType = datatype.getTypeArguments().get(0);
               
        	     final List<Object> udt = Lists.newArrayList();
               if (value != null) {
                   for (Object element : (List<Object>) value) {
                       udt.add(toUdtValue(tablename, catalog, elementDataType, element));
                   }
               }
               
               return ImmutableList.copyOf(udt);
              
           // map
           } else {
               final DataType keyDataType = datatype.getTypeArguments().get(0);
               final DataType valueDataType = datatype.getTypeArguments().get(1);
               
               final Map<Object, Object> udt = Maps.newHashMap();
               if (value != null) {
                   for (Entry<Object, Object> entry : ((Map<Object, Object>) value).entrySet()) {
                         udt.put(toUdtValue(tablename, catalog, keyDataType, entry.getKey()), 
                                 toUdtValue(tablename, catalog, valueDataType, entry.getValue()));
                   }
               
               }
               return ImmutableMap.copyOf(udt);  
           }
    
           
        // udt
        } else {
            if (value == null) {
                return value;
                
            } else {
                final UserType usertype = catalog.getUserType(tablename, ((UserType) datatype).getTypeName());
                final UDTValue udtValue = usertype.newValue();
                
                for (Entry<String, Optional<Object>> entry : beanMapper.toValues(value, ImmutableSet.<String>of()).entrySet()) {
                    if (!entry.getValue().isPresent()) {
                        //return null;
                    	udtValue.setToNull(entry.getKey());
                    	continue;
                    }

                    final DataType fieldType = usertype.getFieldType(entry.getKey());
                    Object vl = entry.getValue().get();
                    
                    if (!isBuildInType(usertype.getFieldType(entry.getKey()))) {
                        vl = toUdtValue(tablename, catalog, fieldType, vl);
                    }
                    
                    final String key = entry.getKey();
                    udtValue.setBytesUnsafe(key, serialize(fieldType, vl));
                }
                
                return udtValue;
            }
        }
    }
    
    
    /**
     * @param tablename  the table name
     * @param name       the columnname
     * @param value      the value 
     * @return the mapped value
     */
    Object toStatementValue(Tablename tablename, String name, Object value) {
        if (isNullOrEmpty(value)) {
            return null;
        } 
        
        final DataType dataType = catalog.getColumnMetadata(tablename, name).getType();
        
        // build in
        if (UDTValueMapper.isBuildInType(dataType)) {
            
            // enum
            if (DataTypes.isTextDataType(dataType) && Enum.class.isAssignableFrom(value.getClass())) {
                return value.toString();
            }
            
            // byte buffer (byte[])
            if (dataType.equals(DataType.blob()) && byte[].class.isAssignableFrom(value.getClass())) {
                return ByteBuffer.wrap((byte[]) value);
            }
            
            return value;
         
        // udt    
        } else {
            return toUdtValue(tablename, catalog, catalog.getColumnMetadata(tablename, name).getType(), value);
        }
    }
    
    
    /**
     * @param tablename   the tablename
     * @param name        the columnname
     * @param values      the vlaues 
     * @return            the mapped values
     */
    ImmutableList<Object> toStatementValues(Tablename tablename, String name, ImmutableList<Object> values) {
        final List<Object> result = Lists.newArrayList(); 

        for (Object value : values) {
            result.add(toStatementValue(tablename, name, value));
        }
        
        return ImmutableList.copyOf(result);
    }

 
    private boolean isNullOrEmpty(Object value) {
        return (value == null) || 
               (Collection.class.isAssignableFrom(value.getClass()) && ((Collection<?>) value).isEmpty()) || 
               (Map.class.isAssignableFrom(value.getClass()) && ((Map<?, ?>) value).isEmpty());
    }
    
        
    /**
	 * Get the CodecRegistry this uses to serialize/deserialize
	 * @return the codecRegistry
	 */
	public CodecRegistry getCodecRegistry() {
		return this.codecRegistry;
	}
	
	/**
	 * Get the metadata catalog this uses
	 * @return the metadata catalog
	 */
	public MetadataCatalog getMetadataCatalog() {
    	return this.catalog;
    }
	
	 /**
     * Serialize a field using the data type passed.
     * @param dataType
     * @param value
     * @return
     */
    @SuppressWarnings("unchecked")
	public <T> ByteBuffer serialize(DataType dataType, Object value) {
        final CodecRegistry codecRegistry = getCodecRegistry();
        final TypeCodec<T> typeCodec = codecRegistry.codecFor(dataType);
    	return typeCodec.serialize((T)value, protocolVersion);
    }
    
    
    /**
     * Serialize a field using the Codec for the value itself
     * @param value
     * @return
     */
    public <T> ByteBuffer serialize(T value) {
        final CodecRegistry codecRegistry = getCodecRegistry();
        final TypeCodec<T> typeCodec = codecRegistry.codecFor(value);
    	return typeCodec.serialize((T)value, protocolVersion);
    }
    
    /**
     * jwestra: 3.x API change
     * deserialize a single field in a UDTValue map
     * @param dataType
     * @param udtValue
     * @param fieldname
     * @return
     */
    public <T> T deserialize(DataType dataType, UDTValue udtValue, String fieldname) {
        final CodecRegistry codecRegistry = getCodecRegistry();
        final TypeCodec<T> typeCodec = codecRegistry.codecFor(dataType);
    	return typeCodec.deserialize(udtValue.getBytesUnsafe(fieldname), protocolVersion);
    }
    
    /**
     * Deserialize a whole ByteBuffer into an object
     * @param dataType
     * @param byteBuffer
     * @return
     */
    public <T> T deserialize(DataType dataType, ByteBuffer byteBuffer) {
        final CodecRegistry codecRegistry = getCodecRegistry();
        final TypeCodec<T> typeCodec = codecRegistry.codecFor(dataType);
    	return typeCodec.deserialize(byteBuffer, protocolVersion);
    }
}   