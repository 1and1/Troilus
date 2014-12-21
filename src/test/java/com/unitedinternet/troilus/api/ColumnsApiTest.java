package com.unitedinternet.troilus.api;




import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;

import org.junit.Assert;
import org.junit.Test;

import com.datastax.driver.core.ConsistencyLevel;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.unitedinternet.troilus.AbstractCassandraBasedTest;
import com.unitedinternet.troilus.AlreadyExistsConflictException;
import com.unitedinternet.troilus.Dao;
import com.unitedinternet.troilus.DaoManager;
import com.unitedinternet.troilus.Write;
import com.unitedinternet.troilus.Record;


public class ColumnsApiTest extends AbstractCassandraBasedTest {
    

        
    
    @Test
    public void testSimpleTable() throws Exception {
        DaoManager daoManager = new DaoManager(getSession());

        Dao userDao = daoManager.getDao(UserTable.TABLE)
                                .withConsistency(ConsistencyLevel.LOCAL_QUORUM);

        


        
        ////////////////
        // inserts
        userDao.write()
               .values(UserTable.USER_ID, "95454", 
                       UserTable.IS_CUSTOMER, true, 
                       UserTable.PICTURE, ByteBuffer.wrap(new byte[] { 8, 4, 3}), 
                       UserTable.ADDRESSES, ImmutableList.of("stuttgart", "baden-baden"), 
                       UserTable.PHONE_NUMBERS, ImmutableSet.of("34234243", "9345324"))
               .execute();
        
        
        userDao.write()
               .values(UserTable.USER_ID, "8345345", UserTable.PHONE_NUMBERS, ImmutableSet.of("24234244"), UserTable.IS_CUSTOMER, true)
               .ifNotExits()
               .withTtl(Duration.ofMinutes(2))
               .withWritetime(Instant.now().toEpochMilli() * 1000)
               .execute();

        
        userDao.write()
               .value(UserTable.USER_ID, "4545")
               .value(UserTable.IS_CUSTOMER, true)
               .value(UserTable.PICTURE, ByteBuffer.wrap(new byte[] { 4, 5, 5}))
               .value(UserTable.ADDRESSES, ImmutableList.of("münchen", "karlsruhe"))
               .value(UserTable.PHONE_NUMBERS, ImmutableSet.of("94665", "34324543"))
               .ifNotExits()       
               .execute();


        try {   // insert twice!
            userDao.write()
                   .value(UserTable.USER_ID, "4545")
                   .value(UserTable.IS_CUSTOMER, true)
                   .value(UserTable.PICTURE, ByteBuffer.wrap(new byte[] { 4, 5, 5}))
                   .value(UserTable.ADDRESSES, ImmutableList.of("münchen", "karlsruhe"))
                   .value(UserTable.PHONE_NUMBERS, ImmutableSet.of("94665", "34324543"))
                   .ifNotExits()       
                   .execute();
            
            Assert.fail("DuplicateEntryException expected"); 
        } catch (AlreadyExistsConflictException expected) { }  

        
        
        userDao.write()
               .value(UserTable.USER_ID, "3434343")
               .value(UserTable.IS_CUSTOMER, Optional.of(true))
               .value(UserTable.PICTURE, ByteBuffer.wrap(new byte[] { 4, 5, 5}))
               .value(UserTable.ADDRESSES, null)
               .execute();


        
        ////////////////
        // reads
        Optional<Record> optionalRecord = userDao.readWithKey(UserTable.USER_ID, "4545")
                                                 .column(UserTable.PICTURE)
                                                 .column(UserTable.ADDRESSES)
                                                 .column(UserTable.PHONE_NUMBERS)
                                                 .execute();
        Assert.assertTrue(optionalRecord.isPresent());
        optionalRecord.ifPresent(record -> System.out.println(record.getList(UserTable.ADDRESSES, String.class).get()));
        System.out.println(optionalRecord.get());
        
        
        
        Optional<Record> optionalRecord2 = userDao.readWithKey(UserTable.USER_ID, "95454")
                                                  .columns(ImmutableList.of(UserTable.PICTURE, UserTable.ADDRESSES, UserTable.PHONE_NUMBERS))
                                                  .execute();
        Assert.assertTrue(optionalRecord2.isPresent());
        optionalRecord2.ifPresent(record -> System.out.println(record.getList(UserTable.ADDRESSES, String.class).get()));

 
        
        Optional<Record> optionalRecord3 = userDao.readWithKey(UserTable.USER_ID, "8345345")
                                                  .column(UserTable.IS_CUSTOMER, true, true)
                                                  .column(UserTable.PICTURE)
                                                  .execute();
        Assert.assertTrue(optionalRecord3.isPresent());
        optionalRecord3.ifPresent(record -> System.out.println(record));

 
       
        

        ////////////////
        // deletes
        userDao.deleteWithKey(UserTable.USER_ID, "4545")
               .execute();
        
        // check
        optionalRecord = userDao.readWithKey(UserTable.USER_ID, "4545")
                                .column(UserTable.USER_ID)
                                .execute();
        Assert.assertFalse(optionalRecord.isPresent());
        
        
        
        
        
        ////////////////
        // batch inserts
        Write insert1 = userDao.write()
                                   .value(UserTable.USER_ID, "14323425")
                                   .value(UserTable.IS_CUSTOMER, true)
                                   .value(UserTable.ADDRESSES, ImmutableList.of("berlin", "budapest"))
                                   .value(UserTable.PHONE_NUMBERS, ImmutableSet.of("12313241243", "232323"));
        
        
        Write insert2 = userDao.write() 
                                   .values(UserTable.USER_ID, "2222", UserTable.IS_CUSTOMER, true, UserTable.ADDRESSES, ImmutableList.of("berlin", "budapest"), UserTable.PHONE_NUMBERS, ImmutableSet.of("12313241243", "232323"));
        
        userDao.write()
               .value(UserTable.USER_ID, "222222")
               .value(UserTable.IS_CUSTOMER, true)
               .value(UserTable.ADDRESSES, ImmutableList.of("hamburg"))
               .value(UserTable.PHONE_NUMBERS, ImmutableSet.of("945453", "23432234"))
               .combinedWith(insert1)
               .combinedWith(insert2)
               .withLockedBatchType()
               .execute();
        
        
        // check
        optionalRecord = userDao.readWithKey(UserTable.USER_ID, "14323425")
                                .execute();
        System.out.println(optionalRecord);
        Assert.assertTrue(optionalRecord.isPresent());

    }               
}


