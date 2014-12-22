package com.unitedinternet.troilus.async;



import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import org.junit.Assert;
import org.junit.Test;

import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.google.common.collect.ImmutableList;
import com.unitedinternet.troilus.AbstractCassandraBasedTest;
import com.unitedinternet.troilus.Dao;
import com.unitedinternet.troilus.DaoManager;
import com.unitedinternet.troilus.Record;
import com.unitedinternet.troilus.TooManyResultsException;
import com.unitedinternet.troilus.api.FeesTable;


public class AsyncTest extends AbstractCassandraBasedTest {
    
    
    
    @Test
    public void testAsync() throws Exception {
        DaoManager daoManager = new DaoManager(getSession());

        
        Dao feeDao = daoManager.getDao(FeesTable.TABLE);

        
        ////////////////
        // inserts
        CompletableFuture<Void> insert1 = feeDao.write()
                                                .value(FeesTable.CUSTOMER_ID, "132")
                                                .value(FeesTable.YEAR, 3)
                                                .value(FeesTable.AMOUNT, 23433)
                                                .executeAsync();
        
        CompletableFuture<Void> insert2 = feeDao.write()
                                                .value(FeesTable.CUSTOMER_ID, "132")
                                                .value(FeesTable.YEAR, 4)
                                                .value(FeesTable.AMOUNT, 1223)
                                                .executeAsync();

        CompletableFuture<Void> insert3 = feeDao.write()
                                                .value(FeesTable.CUSTOMER_ID, "132")
                                                .value(FeesTable.YEAR, 8)
                                                .value(FeesTable.AMOUNT, 23233)
                                                .executeAsync();
        
        CompletableFuture.allOf(insert1, insert2, insert3)
                         .get();  // waits for completion
        
        
        
        try {
            feeDao.readWithKey(FeesTable.CUSTOMER_ID, "132")
                  .columns(FeesTable.ALL)
                  .executeAsync()
                  .get();   // waits for completion
            
            Assert.fail("TooManyResultsException expected");
        } catch (ExecutionException expected) { 
            Assert.assertTrue(expected.getCause() instanceof TooManyResultsException);
        }
        
        
        
        
        ////////////////
        // reads
        ImmutableList<Record> recs = feeDao.readWithCondition(QueryBuilder.eq(FeesTable.CUSTOMER_ID, "132"))
                                           .column(FeesTable.CUSTOMER_ID)
                                           .executeAsync()
                                           .thenApply(result -> ImmutableList.of(result.next(), result.next(), result.next()))
                                           .get();   // waits for completion
        Assert.assertEquals(3, recs.size());
        
        
        

        Record record = feeDao.readWithKey(FeesTable.CUSTOMER_ID, "132", FeesTable.YEAR, 4)
                              .columns(FeesTable.ALL)
                              .executeAsync()
                              .thenApply(optionalRecord -> optionalRecord.<RuntimeException>orElseThrow(RuntimeException::new))
                              .get();   // waits for completion;
        Assert.assertEquals((Integer) 4, record.getInt(FeesTable.YEAR).get());
    }        
}


