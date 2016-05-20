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

import net.oneandone.troilus.Dao;
import net.oneandone.troilus.DaoImpl;
import net.oneandone.troilus.CassandraDB;
import net.oneandone.troilus.Record;
import net.oneandone.troilus.testtables.UsersTable;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.datastax.driver.core.ConsistencyLevel;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;



public class CollectionsTest {
    
    
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
    }

    
    @Test
    public void testSimpleTable() throws Exception {
        Dao usersDao = new DaoImpl(cassandra.getSession(), UsersTable.TABLE)
                                 .withConsistency(ConsistencyLevel.LOCAL_QUORUM);


        String id = "34534535";
        
        ////////////////
        // inserts
        usersDao.writeWithKey(UsersTable.USER_ID, id)
                .value(UsersTable.IS_CUSTOMER, true) 
                .value(UsersTable.PICTURE, ByteBuffer.wrap(new byte[] { 8, 4, 3})) 
                .value(UsersTable.ADDRESSES, ImmutableList.of("stuttgart", "baden-baden")) 
                .value(UsersTable.PHONE_NUMBERS, ImmutableSet.of("34234243", "9345324"))
                .value(UsersTable.ROLES, ImmutableMap.of("r3", "4455", "r7", "242223"))
                .execute();
        
        Record record = usersDao.readWithKey(UsersTable.USER_ID, id)
                                .execute()
                                .get();
        Assert.assertEquals(ImmutableList.of("stuttgart", "baden-baden"), record.getList(UsersTable.ADDRESSES, String.class));
        Assert.assertEquals(ImmutableSet.of("34234243", "9345324"), record.getSet(UsersTable.PHONE_NUMBERS, String.class));
        Assert.assertEquals(ImmutableMap.of("r3", "4455", "r7", "242223"), record.getMap(UsersTable.ROLES, String.class, String.class));


        
        
        // replace 
        usersDao.writeWithKey(UsersTable.USER_ID, id)
                .value(UsersTable.ADDRESSES, ImmutableList.of("london"))         
                .value(UsersTable.PHONE_NUMBERS, ImmutableSet.of("12313323"))    
                .execute();

        record = usersDao.readWithKey(UsersTable.USER_ID, id)
                         .execute()
                         .get();
        Assert.assertEquals(ImmutableList.of("london"), record.getList(UsersTable.ADDRESSES, String.class));
        Assert.assertEquals(ImmutableSet.of("12313323"), record.getSet(UsersTable.PHONE_NUMBERS, String.class));


        // add entry
        usersDao.writeWithKey(UsersTable.USER_ID, id)
                .appendListValue(UsersTable.ADDRESSES, "paris")
                .addSetValue(UsersTable.PHONE_NUMBERS, "323243")
                .putMapValue(UsersTable.ROLES, "r2", "324325")
                .execute();

        record = usersDao.readWithKey(UsersTable.USER_ID, id)
                         .execute()
                         .get();
        Assert.assertEquals(ImmutableList.of("london", "paris"), record.getList(UsersTable.ADDRESSES, String.class));
        Assert.assertEquals(ImmutableSet.of("12313323", "323243"), record.getSet(UsersTable.PHONE_NUMBERS, String.class));
        Assert.assertEquals(ImmutableMap.of("r2", "324325", "r3", "4455", "r7", "242223"), record.getMap(UsersTable.ROLES, String.class, String.class));
        
        

        // add entry
        usersDao.writeWithKey(UsersTable.USER_ID, id)
                .prependListValue(UsersTable.ADDRESSES, "prag")
                .execute();

        record = usersDao.readWithKey(UsersTable.USER_ID, id)
                         .column(UsersTable.ADDRESSES)
                         .column(UsersTable.PHONE_NUMBERS)   
                         .execute()
                         .get();
        Assert.assertEquals(ImmutableList.of("prag", "london", "paris"), record.getList(UsersTable.ADDRESSES, String.class));
        Assert.assertEquals(ImmutableSet.of("12313323", "323243"), record.getSet(UsersTable.PHONE_NUMBERS, String.class));

        
        
        // remove entry
        usersDao.deleteWithKey(UsersTable.USER_ID, id)
                .removeMapValue(UsersTable.ROLES, "r2")
                .execute();

        record = usersDao.readWithKey(UsersTable.USER_ID, id)
                         .execute()
                         .get();
        Assert.assertEquals(ImmutableMap.of("r3", "4455", "r7", "242223"), record.getMap(UsersTable.ROLES, String.class, String.class));
    }        
}


