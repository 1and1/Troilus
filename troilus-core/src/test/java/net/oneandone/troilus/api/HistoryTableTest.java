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

import net.oneandone.troilus.Dao;
import net.oneandone.troilus.DaoImpl;
import net.oneandone.troilus.testtables.HistoryTable;
import net.oneandone.troilus.CassandraDB;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.datastax.driver.core.ConsistencyLevel;



public class HistoryTableTest {
    
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
        cassandra.tryExecuteCqlFile(HistoryTable.DDL);
    }

     
    @Test
    public void testHistoryTable() throws Exception {
        Dao history = new DaoImpl(cassandra.getSession(), HistoryTable.TABLE)
                                .withConsistency(ConsistencyLevel.ONE);
        
        // insert
        history.writeWithKey(HistoryTable.SENDER_EMAIL, "sender@example.org", HistoryTable.RECEIVER_EMAIL, "receiver@example.org")
               .execute();
    }        
}


