package com.unitedinternet.troilus.async;



import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.unitedinternet.troilus.AbstractCassandraBasedTest;
import com.unitedinternet.troilus.Dao;
import com.unitedinternet.troilus.DaoManager;
import com.unitedinternet.troilus.Record;
import com.unitedinternet.troilus.TooManyResultsException;
import com.unitedinternet.troilus.api.FeeTable;


public class AsyncTest extends AbstractCassandraBasedTest {
    
    
    
    @Test
    public void testAsync() throws Exception {
        DaoManager daoManager = new DaoManager(getSession());

        
        Dao feeDao = daoManager.getDao(FeeTable.TABLE);

        
        // insert
        CompletableFuture<Void> insert1 = feeDao.insertValues(FeeTable.CUSTOMER_ID, "132", FeeTable.YEAR, 3, FeeTable.AMOUNT, 23433)
                                                .executeAsync();
        
        CompletableFuture<Void> insert2 = feeDao.insertValues(FeeTable.CUSTOMER_ID, "132", FeeTable.YEAR, 4, FeeTable.AMOUNT, 1223)
                                                .executeAsync();

        CompletableFuture<Void> insert3 = feeDao.insertValues(FeeTable.CUSTOMER_ID, "132", FeeTable.YEAR, 8, FeeTable.AMOUNT, 23233)
                                                .executeAsync();
        
        CompletableFuture.allOf(insert1, insert2, insert3)
                         .get();  // waits for completion
        
        
        
        
        
        try {
            feeDao.readWithKey(FeeTable.CUSTOMER_ID, "132")
                  .columns(FeeTable.ALL)
                  .executeAsync()
                  .get();   // waits for completion
            
            Assert.fail("TooManyResultsException expected");
        } catch (ExecutionException expected) { 
            Assert.assertTrue(expected.getCause() instanceof TooManyResultsException);
        }
        
        
        
        ImmutableList<Record> recs = feeDao.readWithPartialKey(FeeTable.CUSTOMER_ID, "132")
                                           .column(FeeTable.CUSTOMER_ID)
                                           .executeAsync()
                                           .thenApply(result -> ImmutableList.of(result.next(), result.next(), result.next()))
                                           .get();   // waits for completion
        Assert.assertEquals(3, recs.size());
        
        
        

        Record record = feeDao.readWithKey(FeeTable.CUSTOMER_ID, "132", FeeTable.YEAR, 4)
                              .columns(FeeTable.ALL)
                              .executeAsync()
                              .thenApply(optionalRecord -> optionalRecord.<RuntimeException>orElseThrow(RuntimeException::new))
                              .get();   // waits for completion;
        Assert.assertEquals((Integer) 4, record.getInt(FeeTable.YEAR).get());
    }        
}


