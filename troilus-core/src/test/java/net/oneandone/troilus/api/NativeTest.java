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

import net.oneandone.troilus.CassandraDB;
import net.oneandone.troilus.LoginsTable;
import net.oneandone.troilus.PlusLoginsTable;
import net.oneandone.troilus.UsersTable;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;


public class NativeTest {
    
    
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
        cassandra.tryExecuteCqlFile(UsersTable.DDL);
        cassandra.tryExecuteCqlFile(LoginsTable.DDL);
        cassandra.tryExecuteCqlFile(PlusLoginsTable.DDL);
    }

    
    @Test
    public void testSimpleTable() throws Exception {
 
        Session session = cassandra.getSession();
        
        session.execute("insert into " + UsersTable.TABLE + " (user_id, is_customer) VALUES('93434434', true)");
        
        ResultSet rs = session.execute("select is_customer from " + UsersTable.TABLE + " where user_id = '93434434'"); 
        Row row = rs.one();
        Assert.assertTrue(row.getBool("is_customer"));
    }        
}


