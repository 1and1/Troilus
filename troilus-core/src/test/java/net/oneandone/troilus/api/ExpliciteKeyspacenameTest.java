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
import java.util.Optional;

import net.oneandone.troilus.Dao;
import net.oneandone.troilus.DaoImpl;
import net.oneandone.troilus.CassandraDB;
import net.oneandone.troilus.Record;
import net.oneandone.troilus.example.Address;
import net.oneandone.troilus.example.AddressType;
import net.oneandone.troilus.example.ClassifierEnum;
import net.oneandone.troilus.example.Hotel;
import net.oneandone.troilus.example.HotelsTable;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.datastax.driver.core.ConsistencyLevel;
import com.google.common.collect.ImmutableSet;




public class ExpliciteKeyspacenameTest {
    
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
    }
    
    
    
    @Test
    public void testImlicite() throws Exception {
        
        Dao hotelsDao = new DaoImpl(cassandra.getSession(), HotelsTable.TABLE)
                                  .withConsistency(ConsistencyLevel.LOCAL_QUORUM);
        
        
        
        Hotel entity = new Hotel("BUP45544", 
                                 "Corinthia Budapest",
                                 ImmutableSet.of("1", "2", "3", "122", "123", "124", "322", "333"),
                                 Optional.of(ClassifierEnum.FIVE), 
                                 Optional.of("Superb hotel housed in a heritage building - exudes old world charm"),
                                 new Address("Erzsébet körút 43", "Budapest", "1073"),
                                 Optional.empty());
               
        hotelsDao.writeEntity(entity)
                 .execute();      
        
        
        Record record = hotelsDao.readWithKey(HotelsTable.ID, "BUP45544")
                                 .column(HotelsTable.NAME)
                                 .execute()
                                 .get();
        Assert.assertEquals("Corinthia Budapest", record.getString(HotelsTable.NAME));
    }
    
    
    
    @Test
    public void testExplicite() throws Exception {
        
        Dao hotelsDao = new DaoImpl(cassandra.getSession(), cassandra.getKeyspacename() + "." + HotelsTable.TABLE)
                                  .withConsistency(ConsistencyLevel.LOCAL_QUORUM);
        
        
        
        Hotel entity = new Hotel("BUP45544", 
                                 "Corinthia Budapest",
                                 ImmutableSet.of("1", "2", "3", "122", "123", "124", "322", "333"),
                                 Optional.of(ClassifierEnum.FIVE), 
                                 Optional.of("Superb hotel housed in a heritage building - exudes old world charm"),
                                 new Address("Erzsébet körút 43", "Budapest", "1073"),
                                 Optional.empty());
               
        hotelsDao.writeEntity(entity)
                 .execute();      
        
        
        Record record = hotelsDao.readWithKey(HotelsTable.ID, "BUP45544")
                                 .column(HotelsTable.NAME)
                                 .execute()
                                 .get();
        Assert.assertEquals("Corinthia Budapest", record.getString(HotelsTable.NAME));
    }
    
    
    
    
    @Test
    public void testExpliciteWithSessionWithoutAssignedKeyspace() throws Exception {
        
        Dao hotelsDao = new DaoImpl(cassandra.newGobalSession(), cassandra.getKeyspacename() + "." + HotelsTable.TABLE)
                                  .withConsistency(ConsistencyLevel.LOCAL_QUORUM);
        
        
        
        Hotel entity = new Hotel("BUP45544", 
                                 "Corinthia Budapest",
                                 ImmutableSet.of("1", "2", "3", "122", "123", "124", "322", "333"),
                                 Optional.of(ClassifierEnum.FIVE), 
                                 Optional.of("Superb hotel housed in a heritage building - exudes old world charm"),
                                 new Address("Erzsébet körút 43", "Budapest", "1073"),
                                 Optional.empty());
               
        hotelsDao.writeEntity(entity)
                 .execute();      
        
        
        Record record = hotelsDao.readWithKey(HotelsTable.ID, "BUP45544")
                                 .column(HotelsTable.NAME)
                                 .execute()
                                 .get();
        Assert.assertEquals("Corinthia Budapest", record.getString(HotelsTable.NAME));
    }
    
    
    @Test
    public void testExpliciteWithSessionAssignedToOtherKeyspace() throws Exception {
        
        Dao hotelsDao = new DaoImpl(cassandra.newSession("system"), cassandra.getKeyspacename() + "." + HotelsTable.TABLE)
                                  .withConsistency(ConsistencyLevel.LOCAL_QUORUM);
        
        
        
        Hotel entity = new Hotel("BUP45544", 
                                 "Corinthia Budapest",
                                 ImmutableSet.of("1", "2", "3", "122", "123", "124", "322", "333"),
                                 Optional.of(ClassifierEnum.FIVE), 
                                 Optional.of("Superb hotel housed in a heritage building - exudes old world charm"),
                                 new Address("Erzsébet körút 43", "Budapest", "1073"),
                                 Optional.empty());
               
        hotelsDao.writeEntity(entity)
                 .execute();      
        
        
        Record record = hotelsDao.readWithKey(HotelsTable.ID, "BUP45544")
                                 .column(HotelsTable.NAME)
                                 .execute()
                                 .get();
        Assert.assertEquals("Corinthia Budapest", record.getString(HotelsTable.NAME));
    }
}