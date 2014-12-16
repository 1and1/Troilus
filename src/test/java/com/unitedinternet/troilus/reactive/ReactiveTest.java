package com.unitedinternet.troilus.reactive;



import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.Assert;
import org.junit.Test;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.unitedinternet.troilus.AbstractCassandraBasedTest;
import com.unitedinternet.troilus.Dao;
import com.unitedinternet.troilus.DaoManager;
import com.unitedinternet.troilus.Record;
import com.unitedinternet.troilus.api.FeeTable;


public class ReactiveTest extends AbstractCassandraBasedTest {
    
    
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
        
        

        MySubscriber<Record> testSubscriber = new MySubscriber<>();
        
        feeDao.read()
              .columns(FeeTable.ALL)
              .executeAsync()
              .thenAccept(result -> result.subscribe(testSubscriber));
        
        
        ImmutableList<Record> records = testSubscriber.getAll();
        Assert.assertEquals(3,  records.size());
        
    }        
}


