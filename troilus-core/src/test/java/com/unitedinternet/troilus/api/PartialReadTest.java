package com.unitedinternet.troilus.api;


import java.util.Optional;


import org.junit.Assert;
import org.junit.Test;

import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.google.common.collect.ImmutableList;
import com.unitedinternet.troilus.AbstractCassandraBasedTest;
import com.unitedinternet.troilus.Dao;
import com.unitedinternet.troilus.DaoImpl;
import com.unitedinternet.troilus.Record;
import com.unitedinternet.troilus.RecordList;
import com.unitedinternet.troilus.TooManyResultsException;



public class PartialReadTest extends AbstractCassandraBasedTest {
    
 
    
    @Test
    public void testPartialKey() throws Exception {
        Dao feeDao = new DaoImpl(getSession(), FeesTable.TABLE);


        
        // insert
        feeDao.writeWithKey(FeesTable.CUSTOMER_ID, "132", FeesTable.YEAR, 3)
              .value(FeesTable.AMOUNT, 23433)
              .execute();
        
        feeDao.writeWithKey(FeesTable.CUSTOMER_ID, "132", FeesTable.YEAR, 4)
              .value(FeesTable.AMOUNT, 1223)
              .execute();

        feeDao.writeWithKey(FeesTable.CUSTOMER_ID, "132", FeesTable.YEAR, 8)
              .value(FeesTable.AMOUNT, 23233)
              .execute();
        
        
        try {
            feeDao.readWithKey(FeesTable.CUSTOMER_ID, "132")
                  .columns(FeesTable.ALL)
                  .execute();
            
            Assert.fail("TooManyResultsException expected");
        } catch (TooManyResultsException expected) { }
        
        
        
        RecordList list = feeDao.readWhere(QueryBuilder.eq(FeesTable.CUSTOMER_ID, "132"))
                                .column(FeesTable.CUSTOMER_ID)
                                .execute();
        Assert.assertNotNull(list.next());
        Assert.assertNotNull(list.next());
        Assert.assertNotNull(list.next());
        Assert.assertFalse(list.hasNext());

        
        ImmutableList.of(list).forEach(record -> System.out.println(record));
        

        Optional<Record> feeRecord = feeDao.readWithKey(FeesTable.CUSTOMER_ID, "132", FeesTable.YEAR, 4)
                                           .columns(FeesTable.ALL)
                                           .execute();
        Assert.assertTrue(feeRecord.isPresent());
       

        
        list = feeDao.readWhere()
                     .column(FeesTable.CUSTOMER_ID)
                     .withLimit(2)
                     .execute();
        Assert.assertNotNull(list.next());
        Assert.assertNotNull(list.next());
        Assert.assertFalse(list.hasNext());

        
        
        list = feeDao.readWhere()
                .column(FeesTable.CUSTOMER_ID)
                .withLimit(3)
                .execute();
        Assert.assertNotNull(list.next());
        Assert.assertNotNull(list.next());
        Assert.assertNotNull(list.next());
        Assert.assertFalse(list.hasNext());
        
        
        
        
        feeDao.deleteWhere(QueryBuilder.eq(FeesTable.CUSTOMER_ID, "132"), QueryBuilder.eq(FeesTable.YEAR, 4))
              .execute();
        
        
        feeRecord = feeDao.readWithKey(FeesTable.CUSTOMER_ID, "132", FeesTable.YEAR, 4)
                          .columns(FeesTable.ALL)
                          .execute();
        Assert.assertFalse(feeRecord.isPresent());
    }        
}

