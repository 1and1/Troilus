package com.unitedinternet.troilus.api;


import java.util.Optional;

import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.unitedinternet.troilus.AbstractCassandraBasedTest;
import com.unitedinternet.troilus.Dao;
import com.unitedinternet.troilus.DaoManager;
import com.unitedinternet.troilus.Record;
import com.unitedinternet.troilus.Result;
import com.unitedinternet.troilus.TooManyResultsException;



public class PartialReadTest extends AbstractCassandraBasedTest {
    
 
    
    @Test
    public void testPartialKey() throws Exception {
        DaoManager daoManager = new DaoManager(getSession());

        Dao feeDao = daoManager.getDao(FeeTable.TABLE);


        
        // insert
        feeDao.insertValues(FeeTable.CUSTOMER_ID, "132", FeeTable.YEAR, 3, FeeTable.AMOUNT, 23433)
              .execute();
        
        feeDao.insertValues(FeeTable.CUSTOMER_ID, "132", FeeTable.YEAR, 4, FeeTable.AMOUNT, 1223)
              .execute();

        feeDao.insertValues(FeeTable.CUSTOMER_ID, "132", FeeTable.YEAR, 8, FeeTable.AMOUNT, 23233)
              .execute();
        
        
        try {
            feeDao.readWithKey(FeeTable.CUSTOMER_ID, "132")
                  .columns(FeeTable.ALL)
                  .execute();
            
            Assert.fail("TooManyResultsException expected");
        } catch (TooManyResultsException expected) { }
        
        
        
        Result<Record> list = feeDao.readWithPartialKey(FeeTable.CUSTOMER_ID, "132")
                                    .column(FeeTable.CUSTOMER_ID)
                                    .execute();
        Assert.assertNotNull(list.next());
        Assert.assertNotNull(list.next());
        Assert.assertNotNull(list.next());
        Assert.assertFalse(list.hasNext());

        
        ImmutableList.of(list).forEach(record -> System.out.println(record));
        

        Optional<Record> feeRecord = feeDao.readWithKey(FeeTable.CUSTOMER_ID, "132", FeeTable.YEAR, 4)
                                           .columns(FeeTable.ALL)
                                           .execute();
        Assert.assertTrue(feeRecord.isPresent());
       

        
        list = feeDao.read()
                     .column(FeeTable.CUSTOMER_ID)
                     .withLimit(2)
                     .execute();
        Assert.assertNotNull(list.next());
        Assert.assertNotNull(list.next());
        Assert.assertFalse(list.hasNext());

        
        
        list = feeDao.read()
                .column(FeeTable.CUSTOMER_ID)
                .withLimit(3)
                .execute();
        Assert.assertNotNull(list.next());
        Assert.assertNotNull(list.next());
        Assert.assertNotNull(list.next());
        Assert.assertFalse(list.hasNext());
    }        
}


