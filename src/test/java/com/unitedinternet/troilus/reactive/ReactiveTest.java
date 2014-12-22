package com.unitedinternet.troilus.reactive;



import java.util.concurrent.CompletableFuture;

import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.unitedinternet.troilus.AbstractCassandraBasedTest;
import com.unitedinternet.troilus.Dao;
import com.unitedinternet.troilus.DaoManager;
import com.unitedinternet.troilus.Record;
import com.unitedinternet.troilus.api.FeesTable;


public class ReactiveTest extends AbstractCassandraBasedTest {
    
    
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
        
        
        
        ////////////////
        // reads
        MySubscriber<Record> testSubscriber = new MySubscriber<>();
        
        feeDao.readWithCondition()
              .columns(FeesTable.ALL)
              .executeAsync()
              .thenAccept(result -> result.subscribe(testSubscriber));
        
        
        ImmutableList<Record> records = testSubscriber.getAll();
        Assert.assertEquals(3,  records.size());
    }        
}


