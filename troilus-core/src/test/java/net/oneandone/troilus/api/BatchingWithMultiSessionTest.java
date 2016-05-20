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
import java.nio.ByteBuffer;
import java.util.Optional;

import net.oneandone.troilus.Dao;
import net.oneandone.troilus.DaoImpl;
import net.oneandone.troilus.CassandraDB;
import net.oneandone.troilus.Insertion;
import net.oneandone.troilus.Record;
import net.oneandone.troilus.Write;
import net.oneandone.troilus.example.Address;
import net.oneandone.troilus.example.AddressType;
import net.oneandone.troilus.example.ClassifierEnum;
import net.oneandone.troilus.example.Hotel;
import net.oneandone.troilus.example.HotelsTable;
import net.oneandone.troilus.testtables.UsersTable;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.datastax.driver.core.ConsistencyLevel;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;




public class BatchingWithMultiSessionTest {
    
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
        cassandra.tryExecuteCqlFile(AddressType.DDL);
        cassandra.tryExecuteCqlFile(HotelsTable.DDL);
        cassandra.tryExecuteCqlFile(UsersTable.DDL);
    }
    
    
    
    @Test
    public void testBatching() throws Exception {
        Dao hotelsDao = new DaoImpl(cassandra.getSession(), HotelsTable.TABLE)
                                  .withConsistency(ConsistencyLevel.LOCAL_QUORUM);
        
        Dao usersDao = new DaoImpl(cassandra.newSession(), UsersTable.TABLE)
                                   .withConsistency(ConsistencyLevel.LOCAL_QUORUM);

        
        
        Write writeUser = usersDao.writeWithKey(UsersTable.USER_ID, "95454")
                                  .value(UsersTable.IS_CUSTOMER, true) 
                                  .value(UsersTable.PICTURE, ByteBuffer.wrap(new byte[] { 8, 4, 3})) 
                                  .value(UsersTable.ADDRESSES, ImmutableList.of("stuttgart", "baden-baden")) 
                                  .value(UsersTable.PHONE_NUMBERS, ImmutableSet.of("34234243", "9345324"));
             

        
        Hotel entity = new Hotel("BUP45544", 
                                 "Corinthia Budapest",
                                 ImmutableSet.of("1", "2", "3", "122", "123", "124", "322", "333"),
                                 Optional.of(ClassifierEnum.FIVE), 
                                 Optional.of("Superb hotel housed in a heritage building - exudes old world charm"),
                                 new Address("Erzsébet körút 43", "Budapest", "1073"),
                                 Optional.empty());
               
        Insertion writeHotel = hotelsDao.writeEntity(entity)
                                        .withConsistency(ConsistencyLevel.QUORUM);      
       
        writeHotel.combinedWith(writeUser).execute();
        
        
        

        
        
        
        Record record = usersDao.readWithKey(UsersTable.USER_ID, "95454")
                                .column(UsersTable.IS_CUSTOMER)
                                .execute()
                                .get();
        Assert.assertTrue(record.getBool(UsersTable.IS_CUSTOMER));

        
        
        record = hotelsDao.readWithKey(HotelsTable.ID, "BUP45544")
                          .column(HotelsTable.NAME)
                          .execute()
                          .get();
        Assert.assertEquals("Corinthia Budapest", record.getString(HotelsTable.NAME));
    }
}