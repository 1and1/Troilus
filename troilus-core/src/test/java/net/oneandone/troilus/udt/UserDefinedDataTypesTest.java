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
package net.oneandone.troilus.udt;




import java.io.IOException;





import net.oneandone.troilus.CassandraDB;
import net.oneandone.troilus.ColumnName;
import net.oneandone.troilus.Dao;
import net.oneandone.troilus.DaoImpl;
import net.oneandone.troilus.Record;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.datastax.driver.core.ConsistencyLevel;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;



public class UserDefinedDataTypesTest {
    
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
        cassandra.tryExecuteCqlFile(ScoreType.DDL);
        cassandra.tryExecuteCqlFile(ClassifierType.DDL);
        cassandra.tryExecuteCqlFile(AddresslineType.DDL);
        cassandra.tryExecuteCqlFile(AddrType.DDL);
        cassandra.tryExecuteCqlFile(CustomersTable.DDL);
    }
    
    
    @Test
    public void testSimpleTable() throws Exception {
        Dao customersDao = new DaoImpl(cassandra.getSession(), CustomersTable.TABLE)
                                     .withConsistency(ConsistencyLevel.LOCAL_QUORUM);

        

        //////////////// 
        // inserts
        customersDao.writeWithKey(CustomersTable.ID, "95453543534")
                    .value(CustomersTable.NAME, "peter")
                    .value(CustomersTable.ROLES, ImmutableMap.of("user", "rtrtr", "employee", "e33"))
                    .value(CustomersTable.PHONE_NUMBERS, ImmutableSet.of("454545", "2354234324"))
                    .value(CustomersTable.CURRENT_ADDRESS, new Addr(ImmutableList.of(new Addressline("brauerstrasse")), 76336, ImmutableMap.of("a1", new Addressline("alias1"))))
                    .value(CustomersTable.OLD_ADDRESSES, ImmutableSet.of(new Addr(ImmutableList.of(new Addressline("frankfurter ring")), 80445, ImmutableMap.of("a2", new Addressline("alias2")))))
                    .value(CustomersTable.CLASSIFICATION, ImmutableMap.of(new Classifier("reliability"), new Score(23)))
                    .value(CustomersTable.CLASSIFICATION2, ImmutableMap.of(5, new Score(23)))
                    .execute();

        
        Record record = customersDao.readWithKey(CustomersTable.ID, "95453543534")
                                    .execute()
                                    .get();
        
        Assert.assertEquals("peter", record.getString(CustomersTable.NAME));
        Assert.assertTrue(record.getSet(CustomersTable.PHONE_NUMBERS, String.class).contains("454545"));       
        Assert.assertEquals("e33", record.getMap(CustomersTable.ROLES, String .class, String.class).get("employee"));
        Assert.assertEquals("brauerstrasse", record.getValue(CustomersTable.CURRENT_ADDRESS, Addr.class).getLines().get(0).getLine());
        Assert.assertEquals("frankfurter ring", record.getSet(CustomersTable.OLD_ADDRESSES, Addr.class).iterator().next().getLines().get(0).getLine());
        Assert.assertEquals((Integer) 23, record.getMap(CustomersTable.CLASSIFICATION, Classifier.class, Score.class).get(new Classifier("reliability")).getScore());
        Assert.assertEquals((Integer) 23, record.getMap(CustomersTable.CLASSIFICATION2, Integer.class, Score.class).get(5).getScore());
        
        
        
        
        // inserts
        customersDao.writeWithKey(CustomersTable.ID, "4563434434")
                    .value(CustomersTable.NAME, "peter")
                    .execute();

        
        record = customersDao.readWithKey(CustomersTable.ID, "4563434434")
                             .execute()
                             .get();
        Assert.assertTrue(record.getSet(CustomersTable.OLD_ADDRESSES, Addr.class).isEmpty());
        Assert.assertTrue(record.getMap(CustomersTable.CLASSIFICATION, Classifier.class, Score.class).isEmpty());
        Assert.assertTrue(record.getMap(CustomersTable.CLASSIFICATION2, Integer.class, Score.class).isEmpty());
        
        Assert.assertTrue(record.getValue(ColumnName.defineSet("old_addresses", Addr.class)).isEmpty());
        Assert.assertTrue(record.getValue(ColumnName.defineMap("classification", Classifier.class, Score.class)).isEmpty());
        Assert.assertTrue(record.getValue(ColumnName.defineMap("classification", Integer.class, Score.class)).isEmpty());
    }               
}


