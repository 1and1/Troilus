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
package net.oneandone.troilus.api;


import java.io.IOException;
import java.util.Iterator;
import java.util.Optional;



import net.oneandone.troilus.CassandraDB;
import net.oneandone.troilus.Dao;
import net.oneandone.troilus.DaoImpl;
import net.oneandone.troilus.Record;
import net.oneandone.troilus.TooManyResultsException;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.google.common.collect.ImmutableList;



public class PartialReadTest {
    
    private static CassandraDB cassandra;
    
    
    @BeforeClass
    public static void beforeClass() throws IOException {
        cassandra = CassandraDB.newInstance();
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
    public void testPartialKey() throws Exception {
        Dao feeDao = new DaoImpl(cassandra.getSession(), FeesTable.TABLE);


        
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
                  .columns(FeesTable.CUSTOMER_ID, FeesTable.YEAR, FeesTable.AMOUNT)
                  .execute();
            
            Assert.fail("TooManyResultsException expected");
        } catch (TooManyResultsException expected) { }
        
        
        
        Iterator<Record> list = feeDao.readSequenceWhere(QueryBuilder.eq(FeesTable.CUSTOMER_ID, "132"))
                                      .column(FeesTable.CUSTOMER_ID)
                                      .execute()
                                      .iterator();
        Assert.assertNotNull(list.next());
        Assert.assertNotNull(list.next());
        Assert.assertNotNull(list.next());
        Assert.assertFalse(list.hasNext());



        list = feeDao.readSequenceWithKey(FeesTable.CUSTOMER_ID, "132")
                     .column(FeesTable.CUSTOMER_ID)
                     .execute()
                     .iterator();
        Assert.assertNotNull(list.next());
        Assert.assertNotNull(list.next());
        Assert.assertNotNull(list.next());
        Assert.assertFalse(list.hasNext());





        
        
        
        
        
        

        Optional<Record> feeRecord = feeDao.readWithKey(FeesTable.CUSTOMER_ID, "132", FeesTable.YEAR, 4)
                                           .columns(FeesTable.CUSTOMER_ID, FeesTable.YEAR, FeesTable.AMOUNT)
                                           .execute();
        Assert.assertTrue(feeRecord.isPresent());
       

        
        list = feeDao.readSequenceWhere()
                     .column(FeesTable.CUSTOMER_ID)
                     .withLimit(2)
                     .execute()
                     .iterator();
        Assert.assertNotNull(list.next());
        Assert.assertNotNull(list.next());
        Assert.assertFalse(list.hasNext());

        
        
        list = feeDao.readSequenceWhere()
                     .column(FeesTable.CUSTOMER_ID)
                     .withLimit(3)
                     .execute()
                     .iterator();
        Assert.assertNotNull(list.next());
        Assert.assertNotNull(list.next());
        Assert.assertNotNull(list.next());
        Assert.assertFalse(list.hasNext());
        
        

        list = feeDao.readSequenceWithKeys(FeesTable.CUSTOMER_ID, "132", FeesTable.YEAR, ImmutableList.of(3, 4, 6876767))
                     .column(FeesTable.CUSTOMER_ID)
                     .execute()
                     .iterator();
        Assert.assertNotNull(list.next());
        Assert.assertNotNull(list.next());
        Assert.assertFalse(list.hasNext());
        
        
        
        
        
        feeDao.deleteWhere(QueryBuilder.eq(FeesTable.CUSTOMER_ID, "132"), QueryBuilder.eq(FeesTable.YEAR, 4))
              .execute();
        
        
        feeRecord = feeDao.readWithKey(FeesTable.CUSTOMER_ID, "132", FeesTable.YEAR, 4)
                          .columns(FeesTable.CUSTOMER_ID, FeesTable.YEAR, FeesTable.AMOUNT)
                          .execute();
        Assert.assertFalse(feeRecord.isPresent());
    }        
}


