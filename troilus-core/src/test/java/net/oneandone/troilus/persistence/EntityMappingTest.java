package net.oneandone.troilus.persistence;


import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Optional;

import net.oneandone.troilus.AbstractCassandraBasedTest;
import net.oneandone.troilus.Dao;
import net.oneandone.troilus.DaoImpl;
import net.oneandone.troilus.Record;
import net.oneandone.troilus.api.UsersTable;

import org.junit.Assert;
import org.junit.Test;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;


public class EntityMappingTest extends AbstractCassandraBasedTest {
    
    
    @Test
    public void testUserObject() throws Exception {

        Dao userDao = new DaoImpl(getSession(), UsersTable.TABLE)
                                .withConsistency(ConsistencyLevel.ONE);

        
        
        ////////////////
        // inserts
        userDao.writeEntity(new User("4454", "paul", true, ByteBuffer.wrap(new byte[] { 6, 7, 8}), 1345553l, ImmutableSet.of("12313241243", "232323"), ImmutableList.of("berlin", "budapest")))
               .ifNotExists()
               .execute();

        
        
        
        ////////////////
        // reads
        userDao.readWithKey("user_id", "4454")   
               .execute()
               .ifPresent(user -> System.out.println(user));
        
        

        Optional<User> optionalUser =  userDao.readWithKey("user_id", "4454")   
                                              .asEntity(User.class)
                                              .execute();
        
        Assert.assertTrue(optionalUser.isPresent());
        Assert.assertEquals("paul", optionalUser.get().getName());
        Assert.assertTrue(optionalUser.get().isCustomer().get());
        Assert.assertTrue(optionalUser.get().getAddresses().contains("berlin"));
        
    
        
        
        Iterator<User> list = userDao.readWhere()
                                     .asEntity(User.class)
                                     .withLimit(3)        
                                     .execute()
                                     .iterator();
        Assert.assertNotNull(list.next());
        Assert.assertFalse(list.hasNext());
        ImmutableList.copyOf(list).forEach(user -> System.out.println(user.getAddresses()));
        
        
        list = userDao.readWhere(QueryBuilder.eq("user_id", "4454"))
                      .asEntity(User.class)   
                      .execute()
                      .iterator();
        Assert.assertNotNull(list.next());
        Assert.assertFalse(list.hasNext());
        
        
        
        
        
        ////////////////
        // update
        userDao.writeWithKey(UsersTable.USER_ID, "4454")
               .value(UsersTable.NAME, "eric")
               .execute();


        Record user = userDao.readWithKey("user_id", "4454")   
                             .execute()
                             .get();
        Assert.assertEquals("eric", user.getString(UsersTable.NAME).get());
        Assert.assertEquals((Long) 1345553l, user.getLong(UsersTable.MODIFIED).get());
        Assert.assertTrue(optionalUser.get().getAddresses().contains("berlin"));
        
        
        
        ////////////////
        // update
        userDao.writeEntity(new User("4454", null, true, ByteBuffer.wrap(new byte[] { 6, 7, 8}), 1345553l, ImmutableSet.of("12313241243", "232323"), ImmutableList.of()))
               .execute();

 
        user = userDao.readWithKey("user_id", "4454")   
                      .execute()
                      .get(); 
        Assert.assertFalse(user.getString(UsersTable.NAME).isPresent());
        Assert.assertEquals((Long) 1345553l, user.getLong(UsersTable.MODIFIED).get());
        Assert.assertFalse(user.getList(UsersTable.ADDRESSES, String.class).isPresent());
        Assert.assertTrue(user.getSet(UsersTable.PHONE_NUMBERS, String.class).get().contains("12313241243"));



    }        
}


