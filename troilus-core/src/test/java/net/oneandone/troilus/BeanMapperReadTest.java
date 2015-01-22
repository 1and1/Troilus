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

import org.junit.Assert;
import org.junit.Test;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;



public class BeanMapperReadTest {
     
    
    @SuppressWarnings("unchecked")
    @Test
    public void testReadBean() throws Exception {
        
        BeanMapper mapper = new BeanMapper();
        
        ImmutableMap<String, Optional<Object>> result = mapper.toValues(new MyBean("test", 
                                                                                   Optional.fromNullable("guavaOptional"), 
                                                                                   java.util.Optional.of("javaOptional"), 
                                                                                   ImmutableSet.of("s1", "s2"), 
                                                                                   Optional.of(ImmutableSet.of("so1", "so2")),
                                                                                   UserType.GOLD,
                                                                                   Optional.of(UserType.SILVER)));
        Assert.assertEquals("test", result.get("s").get());
        Assert.assertEquals("guavaOptional", result.get("s2").get());
        Assert.assertEquals("javaOptional", result.get("s3").get());
        Assert.assertFalse(result.get("s01").isPresent());
        Assert.assertFalse(result.get("s02").isPresent());
        Assert.assertTrue(((ImmutableSet<String>) result.get("set").get()).contains("s1"));
        Assert.assertTrue(((ImmutableSet<String>) result.get("seto").get()).contains("so1"));
        Assert.assertEquals(UserType.GOLD, result.get("e").get());
        Assert.assertEquals(UserType.SILVER, result.get("oe").get());

    }        
    
    
    
    
    public static final class MyBean {
        
        @Field(name="s")
        private final String s;
    
        
        @Field(name="s2")
        private final Optional<String> s2;
    
        
        @Field(name="s3")
        private final java.util.Optional<String> s3;

        @Field(name="s01")
        private final String s01;

        @Field(name="s02")
        private final Optional<String> s02;


        @Field(name="set")
        private final ImmutableSet<String> set;

        @Field(name="seto")
        private final Optional<ImmutableSet<String>> seto;

        @Field(name="e")
        private final UserType e;

        @Field(name="oe")
        private final Optional<UserType> oe;

        public MyBean(String s, Optional<String> s2, java.util.Optional<String> s3, ImmutableSet<String> set, Optional<ImmutableSet<String>> seto, UserType e, Optional<UserType> oe) {
            this.s = s;
            this.s2 = s2;
            this.s3 = s3;
            this.s01 = null;
            this.s02 = null;
            this.set = set;
            this.seto = seto;
            this.e = e;
            this.oe = oe;
        }
    }
}


