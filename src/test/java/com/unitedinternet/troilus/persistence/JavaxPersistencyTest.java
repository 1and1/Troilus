package com.unitedinternet.troilus.persistence;


import java.nio.ByteBuffer;
import java.util.Optional;

import org.junit.Assert;
import org.junit.Test;

import com.datastax.driver.core.ConsistencyLevel;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.unitedinternet.troilus.AbstractCassandraBasedTest;
import com.unitedinternet.troilus.Dao;
import com.unitedinternet.troilus.DaoManager;
import com.unitedinternet.troilus.Result;
import com.unitedinternet.troilus.api.UserTable;


public class JavaxPersistencyTest extends AbstractCassandraBasedTest {
    
    
    @Test
    public void testUserObject() throws Exception {
        DaoManager daoManager = new DaoManager(getSession());


        Dao userDao = daoManager.getDao(UserTable.TABLE)
                                .withConsistency(ConsistencyLevel.ONE);

        
        
        ////////////////
        // inserts
        userDao.write()
               .entity(new User("4454", "paul", true, ByteBuffer.wrap(new byte[] { 6, 7, 8}), 1345553l, ImmutableSet.of("12313241243", "232323"), ImmutableList.of("berlin", "budapest")))
               .ifNotExits()
               .execute();

        
        
        
        ////////////////
        // reads
        userDao.readWithKey("user_id", "4454")   
               .execute()
               .ifPresent(user -> System.out.println(user));
        
        

        Optional<User> optionalUser =  userDao.readWithKey("user_id", "4454")   
                                              .entity(User.class)
                                              .execute();
        
        Assert.assertTrue(optionalUser.isPresent());
        Assert.assertEquals("paul", optionalUser.get().getName());
        Assert.assertTrue(optionalUser.get().isCustomer().get());
        Assert.assertTrue(optionalUser.get().getAddresses().contains("berlin"));
    //    Assert.assertEquals(ByteBuffer.wrap(new byte[] { 6, 7, 8}).get(1), optionalUser.get().getPicture().get(1));
        
    
        
        
        Result<User> list = userDao.readWithCondition()
                                   .entity(User.class)
                                   .withLimit(3)
                                   .execute();
        Assert.assertNotNull(list.next());
        Assert.assertFalse(list.hasNext());
        ImmutableList.copyOf(list).forEach(user -> System.out.println(user.getAddresses()));
    }        
}


