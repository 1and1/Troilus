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
package net.oneandone.troilus.api;


import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.Instant;
import java.util.Iterator;

import net.oneandone.troilus.CassandraDB;
import net.oneandone.troilus.Dao;
import net.oneandone.troilus.DaoImpl;
import net.oneandone.troilus.Record;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;



public class ReadWithConditionTest {
    
    private static CassandraDB cassandra;
    
    
    @BeforeClass
    public static void beforeClass() throws IOException {
        cassandra = CassandraDB.newInstance();
    }
        
    @AfterClass
    public static void afterClass() throws IOException {
        cassandra.close();
    }


    
    @Before
    public void before() throws IOException {
        cassandra.tryExecuteCqlFile(UsersTable.DDL);
    }

 
    
    @Test
    public void testPartialKey() throws Exception {
        Dao userDao = new DaoImpl(cassandra.getSession(), UsersTable.TABLE)
                                .withConsistency(ConsistencyLevel.ONE);

  
    
        
        // insert
        userDao.writeWithKey(UsersTable.USER_ID, "342342")
               .value(UsersTable.IS_CUSTOMER, true)
               .value(UsersTable.PICTURE, ByteBuffer.wrap(new byte[] { 8, 4, 3}))
               .value(UsersTable.ADDRESSES, ImmutableList.of("stuttgart", "baden-baden"))
               .value(UsersTable.PHONE_NUMBERS, ImmutableSet.of("34234243", "9345324"))
               .execute();
        
        
        userDao.writeWithKey(UsersTable.USER_ID, "2334233")
               .value(UsersTable.PHONE_NUMBERS, ImmutableSet.of("24234244"))
               .value(UsersTable.IS_CUSTOMER, true)
               .ifNotExists()
               .withTtl(Duration.ofMinutes(2))
               .withWritetime(Instant.now().toEpochMilli() * 1000)
               .execute();
        
        
        userDao.writeWithKey(UsersTable.USER_ID, "935434")
               .value(UsersTable.PHONE_NUMBERS, ImmutableSet.of("24234244"))
               .value(UsersTable.IS_CUSTOMER, true)
               .ifNotExists()
               .withTtl(Duration.ofMinutes(2))
               .withWritetime(Instant.now().toEpochMilli() * 1000)
               .execute();


        
        userDao.writeWithKey(UsersTable.USER_ID, "2323")
               .value(UsersTable.PHONE_NUMBERS, ImmutableSet.of("24234244"))
               .value(UsersTable.IS_CUSTOMER, true)
               .ifNotExists()
               .withTtl(Duration.ofMinutes(2))
               .withWritetime(Instant.now().toEpochMilli() * 1000)
               .execute();




        Iterator<Record> records = userDao.readWhere(QueryBuilder.in(UsersTable.USER_ID, "2323", "935434"))
                                          .withAllowFiltering()
                                          .execute()
                                          .iterator();
        
        Assert.assertEquals(true, records.next().getBool(UsersTable.IS_CUSTOMER));
        Assert.assertEquals(true, records.next().getBool(UsersTable.IS_CUSTOMER));
        Assert.assertFalse(records.hasNext());
    }        
}


