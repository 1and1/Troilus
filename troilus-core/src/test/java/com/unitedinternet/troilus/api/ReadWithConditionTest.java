package com.unitedinternet.troilus.api;


import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.Instant;
import java.util.Iterator;

import org.junit.Assert;
import org.junit.Test;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.unitedinternet.troilus.AbstractCassandraBasedTest;
import com.unitedinternet.troilus.Dao;
import com.unitedinternet.troilus.DaoImpl;
import com.unitedinternet.troilus.Record;



public class ReadWithConditionTest extends AbstractCassandraBasedTest {
    
 
    
    @Test
    public void testPartialKey() throws Exception {
        Dao userDao = new DaoImpl(getSession(), UsersTable.TABLE)
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
        
        Assert.assertEquals(true, records.next().getBool(UsersTable.IS_CUSTOMER).get());
        Assert.assertEquals(true, records.next().getBool(UsersTable.IS_CUSTOMER).get());
        Assert.assertFalse(records.hasNext());
        


        
    }        
}


