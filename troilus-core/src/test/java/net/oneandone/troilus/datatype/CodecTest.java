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
package net.oneandone.troilus.datatype;



import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Instant;

import net.oneandone.troilus.Dao;
import net.oneandone.troilus.DaoImpl;
import net.oneandone.troilus.LoginsTable;
import net.oneandone.troilus.PlusLoginsTable;
import net.oneandone.troilus.CassandraDB;
import net.oneandone.troilus.Record;
import net.oneandone.troilus.UsersTable;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.datastax.driver.core.ConsistencyLevel;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;



public class CodecTest {
    
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
    public void testJava8Codec() throws Exception {
        Dao usersDao = new DaoImpl(cassandra.getSession(), UsersTable.TABLE)
                                 .withConsistency(ConsistencyLevel.LOCAL_QUORUM);

        
        ////////////////
        // inserts
        usersDao.writeWithKey(UsersTable.USER_ID, "4545545")
                .value(UsersTable.IS_CUSTOMER, true) 
                .value(UsersTable.MODIFIED, Instant.ofEpochMilli(1458908624228l))
                .value(UsersTable.PICTURE, ByteBuffer.wrap(new byte[] { 8, 4, 3})) 
                .value(UsersTable.ADDRESSES, ImmutableList.of("stuttgart", "baden-baden")) 
                .value(UsersTable.PHONE_NUMBERS, ImmutableSet.of("34234243", "9345324"))
                .execute();
        
        
        Record record = usersDao.readWithKey(UsersTable.USER_ID, "4545545")
                                .execute()
                                .get();
        Assert.assertEquals(1458908624228l, record.getInstant(UsersTable.MODIFIED).toEpochMilli());
    }        
}


