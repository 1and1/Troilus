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
package net.oneandone.troilus.reactive;



import java.io.IOException;

import java.util.concurrent.CompletableFuture;

import net.oneandone.troilus.CassandraDB;
import net.oneandone.troilus.Dao;
import net.oneandone.troilus.DaoImpl;
import net.oneandone.troilus.Record;
import net.oneandone.troilus.Result;
import net.oneandone.troilus.api.FeesTable;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.google.common.collect.ImmutableList;



public class ReactiveTest {
    
    private static CassandraDB cassandra;
    
    
    @BeforeClass
    public static void beforeClass() throws IOException {
        cassandra = CassandraDB.create();
    }
        
    @AfterClass
    public static void afterClass() throws IOException {
        cassandra.close();
    }

    
    @Before
    public void before() throws IOException {
        cassandra.tryExecuteCqlFile(FeesTable.DDL);
    }
    

    
    @Test
    public void testAsync() throws Exception {
        Dao feeDao = new DaoImpl(cassandra.getSession(), FeesTable.TABLE);

        
        ////////////////
        // inserts
        CompletableFuture<Result> insert1 = feeDao.writeWithKey(FeesTable.CUSTOMER_ID, "9565464", FeesTable.YEAR, 3)
                                                  .value(FeesTable.AMOUNT, 23433)
                                                  .executeAsync();
        
        CompletableFuture<Result> insert2 = feeDao.writeWithKey(FeesTable.CUSTOMER_ID, "9565464", FeesTable.YEAR, 4)
                                                  .value(FeesTable.AMOUNT, 1223)
                                                  .executeAsync();

        CompletableFuture<Result> insert3 = feeDao.writeWithKey(FeesTable.CUSTOMER_ID, "9565464", FeesTable.YEAR, 8)
                                                  .value(FeesTable.AMOUNT, 23233)
                                                  .executeAsync();
        
        CompletableFuture.allOf(insert1, insert2, insert3)
                         .get();  // waits for completion
        
        
        
        ////////////////
        // reads
        MySubscriber testSubscriber = new MySubscriber();
        
        feeDao.readWhere(QueryBuilder.eq(FeesTable.CUSTOMER_ID, "9565464"))
              .columns(FeesTable.CUSTOMER_ID, FeesTable.YEAR, FeesTable.AMOUNT)
              .executeAsync()
              .thenAccept(result -> result.subscribe(testSubscriber));
        
        
        ImmutableList<Record> records = testSubscriber.getAll();
        if (records.size() != 3) {
            for (Record record : records) {
                System.out.println("" + record);
            }
            
            Assert.fail("expect 3 records. got " + records.size());
        }
    }        
}


