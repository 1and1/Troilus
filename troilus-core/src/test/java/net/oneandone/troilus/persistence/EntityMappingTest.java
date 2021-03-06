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
import java.time.Duration;
import java.util.Iterator;
import java.util.Optional;

import net.oneandone.troilus.CassandraDB;
import net.oneandone.troilus.Dao;
import net.oneandone.troilus.DaoImpl;
import net.oneandone.troilus.Record;
import net.oneandone.troilus.api.UsersTable;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.Uninterruptibles;


public class EntityMappingTest  {
    
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
    public void testUserObject() throws Exception {

        Dao userDao = new DaoImpl(cassandra.getSession(), UsersTable.TABLE)
                                .withConsistency(ConsistencyLevel.ONE);

        
        
        ////////////////
        // inserts
        userDao.writeEntity(new User("67454545", "paul", true, ByteBuffer.wrap(new byte[] { 6, 7, 8}), new byte[] { 5, 7, 8, 5}, 1345553l, ImmutableSet.of("12313241243", "232323"), ImmutableList.of("berlin", "budapest")))
               .ifNotExists()
               .execute();
        
        
        
        ////////////////
        // reads
        userDao.readWithKey("user_id", "67454545")   
               .execute()
               .ifPresent(user -> System.out.println(user));
        
        

        Optional<User> optionalUser =  userDao.readWithKey("user_id", "67454545")   
                                              .asEntity(User.class)
                                              .execute();
        
        Assert.assertTrue(optionalUser.isPresent());
        Assert.assertEquals("paul", optionalUser.get().getName());
        Assert.assertArrayEquals(new byte[] { 5, 7, 8, 5}, optionalUser.get().getSecId().get());
        Assert.assertTrue(optionalUser.get().isCustomer().get());
        Assert.assertTrue(optionalUser.get().getAddresses().contains("berlin"));
        
    
        
        
        Iterator<User> list = userDao.readSequenceWhere()
                                     .asEntity(User.class)
                                     .withLimit(3)        
                                     .execute()
                                     .iterator();
        Assert.assertNotNull(list.next());
        Assert.assertFalse(list.hasNext());
        ImmutableList.copyOf(list).forEach(user -> System.out.println(user.getAddresses()));
        
        
            
        Iterator<Record> records = userDao.readSequenceWhere(QueryBuilder.eq("user_id", "67454545"))
                                          .execute()
                                          .iterator();
        
        Record record = records.next();
        Assert.assertEquals("67454545", record.getString("user_Id"));
        Assert.assertEquals(true, record.getBool("is_customer"));
        Assert.assertNotNull(record.getLong("modified"));
        Assert.assertTrue(record.getSet("phone_numbers", String.class).contains("12313241243"));
        Assert.assertArrayEquals(new byte[] { 6, 7, 8}, record.getValue("picture", byte[].class));
        Assert.assertFalse(records.hasNext());
        
        
        
        
        list = userDao.readSequenceWhere(QueryBuilder.eq("user_id", "67454545"))
                      .asEntity(User.class)   
                      .execute()
                      .iterator();
        Assert.assertNotNull(list.next());
        Assert.assertFalse(list.hasNext());
        
        
        
        
        
        optionalUser =  Uninterruptibles.getUninterruptibly(userDao.readWithKey("user_id", "non_existing")
                                                                   .asEntity(User.class)   
                                                                   .executeAsync());
        Assert.assertFalse(optionalUser.isPresent());


        
        optionalUser = userDao.readWithKey("user_id", "non_existing")
                              .asEntity(User.class)  
                              .execute();
        Assert.assertFalse(optionalUser.isPresent());

        
        
        ////////////////
        // update
        userDao.writeWithKey(UsersTable.USER_ID, "67454545")
               .value(UsersTable.NAME, "eric")
               .execute();


        Record user = userDao.readWithKey("user_id", "67454545")   
                             .execute()
                             .get();
        Assert.assertEquals("eric", user.getString(UsersTable.NAME));
        Assert.assertEquals(1345553, user.getLong(UsersTable.MODIFIED));
        
        
        
        ////////////////
        // update
        userDao.writeEntity(new User("67454545", null, true, ByteBuffer.wrap(new byte[] { 6, 7, 8}), new byte[] { 5, 7, 8, 5}, 1345553l, ImmutableSet.of("12313241243", "232323"), ImmutableList.of()))
               .execute();

 
        user = userDao.readWithKey("user_id", "67454545")   
                      .execute()
                      .get(); 
        Assert.assertNull(user.getString(UsersTable.NAME));
        Assert.assertEquals(1345553, user.getLong(UsersTable.MODIFIED));
        Assert.assertTrue(user.getList(UsersTable.ADDRESSES, String.class).isEmpty());
        Assert.assertTrue(user.getSet(UsersTable.PHONE_NUMBERS, String.class).contains("12313241243"));


        
        ////////////////
        // inserts
        userDao.writeWithKey("user_id", "568785563")
               .entity(new MinimalUser("paul", true, ByteBuffer.wrap(new byte[] { 6, 7, 8}), new byte[] { 5, 7, 8, 5}, 1345553l, ImmutableSet.of("12313241243", "232323"), ImmutableList.of("berlin", "budapest")))
               .execute();

        
        
        
        ////////////////
        // reads
        User usr = userDao.readWithKey("user_id", "568785563")   
                          .asEntity(User.class)
                          .execute()
                          .get();
                      
        Assert.assertEquals("paul", usr.getName());
        
        
        

        
        userDao.writeEntity(new User("435675743", "bertra", true, ByteBuffer.wrap(new byte[] { 6, 7, 8}), null, 1345553l, ImmutableSet.of("12313241243", "232323"), ImmutableList.of("berlin", "budapest")))
               .ifNotExists()
               .execute();
        
        usr = userDao.readWithKey("user_id", "435675743")   
                     .asEntity(User.class)
                     .execute()
                     .get();
            
        Assert.assertFalse(usr.getSecId().isPresent());
        
        
        
        userDao.writeEntity(new User("232443443", "paul", true, ByteBuffer.wrap(new byte[] { 6, 7, 8}), new byte[] { 5, 7, 8, 5}, 1345553l, ImmutableSet.of("12313241243", "232323"), ImmutableList.of("berlin", "budapest")))
               .ifNotExists()
               .withTtl(Duration.ofSeconds(11))
               .execute();
    }        
}


