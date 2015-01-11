package com.unitedinternet.troilus;

import org.junit.Assert;
import org.junit.Test;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;


public class BeanMapperWriteTest {
     
    
    @Test
    public void testWriteBean() throws Exception {
        
        BeanMapper mapper = new BeanMapper();
        
        MyBean bean = mapper.fromValues(MyBean.class, SimplePropertySource.newSource(ImmutableMap.of("s", Optional.of("test"))));
        Assert.assertEquals("test", bean.getS());
        
        bean = mapper.fromValues(MyBean.class, SimplePropertySource.newSource(ImmutableMap.of("s", Optional.absent())));
        Assert.assertNull(bean.getS());
        
        
        
        bean = mapper.fromValues(MyBean.class, SimplePropertySource.newSource(ImmutableMap.of("so", Optional.of("test"))));
        Assert.assertEquals("test", bean.getSo().get());
        
        bean = mapper.fromValues(MyBean.class, SimplePropertySource.newSource(ImmutableMap.of("so", Optional.absent())));
        Assert.assertFalse(bean.getSo().isPresent());
        
        
        
        bean = mapper.fromValues(MyBean.class, SimplePropertySource.newSource(ImmutableMap.of("sj", Optional.of("test"))));
        Assert.assertEquals("test", bean.getSj().get());
        
        bean = mapper.fromValues(MyBean.class, SimplePropertySource.newSource(ImmutableMap.of("sj", Optional.absent())));
        Assert.assertFalse(bean.getSj().isPresent());
        
        

        bean = mapper.fromValues(MyBean.class, SimplePropertySource.newSource(ImmutableMap.of("set", Optional.of(ImmutableSet.of("set1", "set2")))));
        Assert.assertTrue(bean.getSet().contains("set1"));
        
        bean = mapper.fromValues(MyBean.class, SimplePropertySource.newSource(ImmutableMap.of("set", Optional.absent())));
        Assert.assertNull(bean.getSet());
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
        public <T> Optional<T> read(String name, Class<Object> clazz1, Class<Object> clazz2) {
            if (properties.get(name) == null) {
                return Optional.absent();
            }
            
            return (Optional<T>) properties.get(name);
        }
    }
}


