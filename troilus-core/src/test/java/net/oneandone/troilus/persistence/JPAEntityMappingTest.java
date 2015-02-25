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
package net.oneandone.troilus.persistence;


import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Optional;

import net.oneandone.troilus.AbstractCassandraBasedTest;
import net.oneandone.troilus.Dao;
import net.oneandone.troilus.DaoImpl;
import net.oneandone.troilus.api.UsersTable;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.datastax.driver.core.ConsistencyLevel;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;


public class JPAEntityMappingTest extends AbstractCassandraBasedTest {
    
    
    @Before
    public void before() throws IOException {
        tryExecuteCqlFile(UsersTable.DDL);
    }
    
    
    
    @Test
    public void testUserObject() throws Exception {

        Dao userDao = new DaoImpl(getSession(), UsersTable.TABLE)
                                .withConsistency(ConsistencyLevel.ONE);

        
        
        ////////////////
        // inserts
        userDao.writeEntity(new JPAUser("234324242", "paul", true, ByteBuffer.wrap(new byte[] { 6, 7, 8}), 1345553l, ImmutableSet.of("12313241243", "232323"), ImmutableList.of("berlin", "budapest")))
               .ifNotExists()
               .execute();

        
        
        
        ////////////////
        // reads

        Optional<JPAUser> optionalUser =  userDao.readWithKey("user_id", "234324242")   
                                                 .asEntity(JPAUser.class)
                                                 .execute();
        
        Assert.assertTrue(optionalUser.isPresent());
        Assert.assertEquals("paul", optionalUser.get().getName());
        Assert.assertTrue(optionalUser.get().isCustomer().get());
        Assert.assertTrue(optionalUser.get().getAddresses().contains("berlin"));
        
    
        
        
        Iterator<JPAUser> list = userDao.readWhere()
                                        .asEntity(JPAUser.class)
                                        .withLimit(3)        
                                        .execute()
                                        .iterator();
        Assert.assertNotNull(list.next());
        Assert.assertFalse(list.hasNext());
        ImmutableList.copyOf(list).forEach(user -> System.out.println(user.getAddresses()));
    }        
}


