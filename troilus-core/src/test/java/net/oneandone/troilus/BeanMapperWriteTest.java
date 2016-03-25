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

import net.oneandone.troilus.BeanMapper;
import net.oneandone.troilus.Field;
import net.oneandone.troilus.PropertiesSource;

import java.util.Optional;

import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;


public class BeanMapperWriteTest {
     
    
    @Test
    public void testWriteBean() throws Exception {
        
        BeanMapper mapper = new BeanMapper();
        
        MyBean bean = mapper.fromValues(MyBean.class, SimplePropertySource.newSource(ImmutableMap.of("s", Optional.of("test"))), ImmutableSet.of());
        Assert.assertEquals("test", bean.getS());
        
        bean = mapper.fromValues(MyBean.class, SimplePropertySource.newSource(ImmutableMap.of("s", Optional.empty())), ImmutableSet.of());
        Assert.assertNull(bean.getS());
        
        
        
        bean = mapper.fromValues(MyBean.class, SimplePropertySource.newSource(ImmutableMap.of("so", Optional.of("test"))), ImmutableSet.of());
        Assert.assertEquals("test", bean.getSo().get());
        
        bean = mapper.fromValues(MyBean.class, SimplePropertySource.newSource(ImmutableMap.of("so", Optional.empty())), ImmutableSet.of());
        Assert.assertFalse(bean.getSo().isPresent());
        
        
        
        bean = mapper.fromValues(MyBean.class, SimplePropertySource.newSource(ImmutableMap.of("sj", Optional.of("test"))), ImmutableSet.of());
        Assert.assertEquals("test", bean.getSj().get());
        
        bean = mapper.fromValues(MyBean.class, SimplePropertySource.newSource(ImmutableMap.of("sj", Optional.empty())), ImmutableSet.of());
        Assert.assertFalse(bean.getSj().isPresent());
        
        

        bean = mapper.fromValues(MyBean.class, SimplePropertySource.newSource(ImmutableMap.of("set", Optional.of(ImmutableSet.of("set1", "set2")))), ImmutableSet.of());
        Assert.assertTrue(bean.getSet().contains("set1"));
        
        bean = mapper.fromValues(MyBean.class, SimplePropertySource.newSource(ImmutableMap.of("set", Optional.empty())), ImmutableSet.of());
        Assert.assertNull(bean.getSet());
        
        
        
        bean = mapper.fromValues(MyBean.class, SimplePropertySource.newSource(ImmutableMap.of("e", Optional.of(UserType.GOLD))), ImmutableSet.of());
        Assert.assertEquals(UserType.GOLD, bean.getE());
        
        bean = mapper.fromValues(MyBean.class, SimplePropertySource.newSource(ImmutableMap.of("oe", Optional.of(UserType.GOLD))), ImmutableSet.of());
        Assert.assertEquals(UserType.GOLD, bean.getOe().get());
    }        
    
    
    
    
    public static final class MyBean {
        
        @Field(name="s")
        private String s;
        
        @Field(name="so")
        private Optional<String> so;

        @Field(name="sj")
        private java.util.Optional<String> sj;
       
        @Field(name="set")
        private ImmutableSet<String> set;
        
        @Field(name="e")
        private UserType e;

        @Field(name="oe")
        private Optional<UserType> oe;

        
        
        public String getS() {
            return s;
        }
        
        public Optional<String> getSo() {
            return so;
        }
        
        public java.util.Optional<String> getSj() {
            return sj;
        }
        
        public ImmutableSet<String> getSet() {
            return set;
        }
        
        public UserType getE() {
            return e;
        }
        
        public Optional<UserType> getOe() {
            return oe;
        }
    }
    
    
    
    private static final class SimplePropertySource implements PropertiesSource {
        
        private final ImmutableMap<String, Optional<Object>> properties; 
        
        public SimplePropertySource(ImmutableMap<String, Optional<Object>> properties) {
            this.properties = properties;
        }
        
        static SimplePropertySource newSource(ImmutableMap<String, Optional<Object>> properties) {
            return new SimplePropertySource(properties);
        }

        @Override
        public <T> Optional<T> read(String name, Class<?> clazz1) {
            return read(name, clazz1, Object.class);
        }
        
        @SuppressWarnings("unchecked")
        @Override
        public <T> Optional<T> read(String name, Class<?> clazz1, Class<?> clazz2) {
            if (properties.get(name) == null) {
                return Optional.empty();
            }
            
            return (Optional<T>) properties.get(name);
        }
    }
}


