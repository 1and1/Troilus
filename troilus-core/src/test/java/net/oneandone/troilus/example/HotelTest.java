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
package net.oneandone.troilus.example;


import java.io.IOException;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import net.oneandone.troilus.Cassandra;
import net.oneandone.troilus.Dao;
import net.oneandone.troilus.DaoImpl;
import net.oneandone.troilus.IfConditionException;
import net.oneandone.troilus.Record;
import net.oneandone.troilus.Result;
import net.oneandone.troilus.Deletion;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.google.common.collect.ImmutableSet;

import static net.oneandone.troilus.example.HotelTableFields.*;


public class HotelTest {
    
    private static Cassandra cassandra;
    
    
    @BeforeClass
    public static void beforeClass() throws IOException {
        cassandra = Cassandra.create();
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
    public void testExample() throws Exception {
                
        // create dao
        Dao hotelsDao = new DaoImpl(cassandra.getSession(), HotelsTable.TABLE)
                                  .withConsistency(ConsistencyLevel.LOCAL_QUORUM)
                                  .withInterceptor(HotelsTable.CONSTRAINTS);
        
        
        ////////////////
        // inserts
        
        Hotel entity = new Hotel("BUP45544", 
                                 "Corinthia Budapest",
                                 ImmutableSet.of("1", "2", "3", "122", "123", "124", "322", "333"),
                                 Optional.of(ClassifierEnum.FIVE), 
                                 Optional.of("Superb hotel housed in a heritage building - exudes old world charm"),
                                 new Address("Erzsébet körút 43", "Budapest", "1073"));
               
        hotelsDao.writeEntity(entity)
                 .ifNotExists()
                 .withConsistency(ConsistencyLevel.QUORUM)      
                 .withSerialConsistency(ConsistencyLevel.SERIAL)
                 .withWritetime(System.currentTimeMillis() * 10000)
                 .withTtl(Duration.ofDays(222))
                 .execute();

        
        try {
            hotelsDao.writeEntity(entity)
                     .ifNotExists()
                     .withConsistency(ConsistencyLevel.QUORUM)      
                     .withSerialConsistency(ConsistencyLevel.SERIAL)
                     .withWritetime(System.currentTimeMillis() * 10000)
                     .withTtl(Duration.ofDays(222))
                     .execute();
        } catch (IfConditionException ice) {
            // ...
        }
        
        Record record = hotelsDao.readWithKey(HotelsTable.ID, "BUP45544")
                                 .column(HotelsTable.NAME)
                                 .columnWithMetadata(HotelsTable.DESCRIPTION)
                                 .execute()
                                 .get();
        Assert.assertNotNull(record.getWritetime(HotelsTable.DESCRIPTION));
        Assert.assertNull(record.getWritetime(HotelsTable.NAME));
        Assert.assertNotNull(record.getTtl(HotelsTable.DESCRIPTION));
        Assert.assertNull(record.getTtl(HotelsTable.NAME));
        
        
        
      
        
        hotelsDao.writeWithKey("id", "BUP932432")
                 .value("name", "City Budapest")
                 .value("room_ids", ImmutableSet.of("1", "2", "3", "4", "5"))
                 .value("classification", ClassifierEnum.FOUR)
                 .value("address", new Address("Andrássy út", "Budapest", "1061"))
                 .ifNotExists()
                 .execute();

       
        // insert async
        CompletableFuture<Result> future = hotelsDao.writeEntity(new Hotel("BUP14334", 
                                                                           "Richter Panzio",
                                                                           ImmutableSet.of("1", "2", "3"),
                                                                           Optional.of(ClassifierEnum.TWO), 
                                                                           Optional.empty(),
                                                                           new Address("Thököly Ut 111", "Budapest", "1145")))
                                                    .withConsistency(ConsistencyLevel.ANY)
                                                    .executeAsync();
        future.get(); // waits for completion
        
        
   
        
        
        ////////////////
        // reads
        hotelsDao.readAll()
                 .asEntity(Hotel.class)
                 .withLimit(100)
                 .executeAsync()
                 .thenAccept(hotelIterator -> hotelIterator.forEach(hotel -> System.out.println(hotel)));
        
        
        
        
        HotelSubscriber mySubscriber = new HotelSubscriber();
        hotelsDao.readWhere()
                 .asEntity(Hotel.class)
                 .withLimit(100)
                 .executeAsync()
                 .thenAccept(hotels -> hotels.subscribe(mySubscriber));
        
        
        for (Hotel hotel : mySubscriber.getAll()) {
            System.out.println(hotel);
        }
        
        
        Iterable<Hotel> hotelIterator = hotelsDao.readAll()
                                                 .asEntity(Hotel.class)
                                                 .withLimit(100)
                                                 .execute();
        hotelIterator.forEach(hotel -> System.out.println(hotel));
        
        
        
        
        hotelIterator = hotelsDao.readWhere(QueryBuilder.in("ID", "BUP45544", "BUP14334"))
                                 .asEntity(Hotel.class)
                                 .withAllowFiltering()
                                 .execute();
        hotelIterator.forEach(hotel -> System.out.println(hotel));
        
        

        
        
        Optional<Hotel> optionalHotel = hotelsDao.readWithKey("id", "BUP45544")
                                                 .asEntity(Hotel.class)
                                                 .withConsistency(ConsistencyLevel.LOCAL_ONE)
                                                 .execute();
        optionalHotel.ifPresent(hotel -> System.out.println(hotel.getName()));
        Assert.assertEquals(8, optionalHotel.get().getRoomIds().size());
        
        
       
        Hotel hotel = hotelsDao.readWithKey("id", "BUP932432")
                               .asEntity(Hotel.class)
                               .executeAsync()
                               .thenApply(opHotel -> opHotel.<RuntimeException>orElseThrow(RuntimeException::new))
                               .get();  // waits for completion
        System.out.println(hotel);

        Assert.assertEquals("1061", hotel.getAddress().getPostCode());



   
        
        
        record = hotelsDao.readWithKey("id", "BUP14334")
                          .column("id")
                          .column("name")
                          .column("classification")
                          .column("address")
                          .withConsistency(ConsistencyLevel.LOCAL_ONE)
                          .execute()
                          .get();
        
        Assert.assertNotNull(record.getValue("address", Address.class));
        Assert.assertEquals("TWO",  record.getString("classification"));
        Assert.assertEquals("Budapest", record.getValue("address", Address.class).getCity());


        
        
        hotel = hotelsDao.readWithKey("id", "BUP14334")
                         .asEntity(Hotel.class)
                         .execute()
                         .get();
        
        Assert.assertNotNull(hotel.getAddress());
        System.out.println(hotel.getAddress());



        
        
        ////////////////
        // updates
        
        hotelsDao.writeWithKey(HotelsTable.ID,"BUP932432")
                 .value(HotelsTable.DESCRIPTION, "The City Budapest is in the business district on the Pest side of the river.")
                 .execute();
                 
        
        record = hotelsDao.readWithKey(HotelsTable.ID, "BUP932432")
                          .column(HotelsTable.NAME)
                          .column(HotelsTable.DESCRIPTION)
                          .execute()
                          .get();
        Assert.assertEquals("City Budapest", record.getString(HotelsTable.NAME));
        Assert.assertEquals("The City Budapest is in the business district on the Pest side of the river.", record.getString(HotelsTable.DESCRIPTION));
               
        
        
        hotelsDao.writeWithKey(HotelsTable.ID,"BUP932432")
                 .value(HotelsTable.DESCRIPTION, Optional.empty())
                 .execute();
        

        record = hotelsDao.readWithKey(HotelsTable.ID, "BUP932432")
                          .column(HotelsTable.NAME)
                          .column(HotelsTable.DESCRIPTION)
                          .column(HotelsTable.CLASSIFICATION)
                          .execute()
                          .get();
        
        Assert.assertEquals(ClassifierEnum.FOUR.toString(), record.getString(HotelsTable.CLASSIFICATION));
        Assert.assertEquals("City Budapest", record.getString(HotelsTable.NAME));
        Assert.assertNull(record.getString(HotelsTable.DESCRIPTION));
        
        
        hotelsDao.writeWithKey(HotelsTable.ID, "BUP932432")
                 .value(HotelsTable.NAME, "Budapest City")
                 .onlyIf(QueryBuilder.eq(HotelsTable.NAME, "City Budapest"))
                 .withSerialConsistency(ConsistencyLevel.SERIAL)
                 .execute();


        record = hotelsDao.readWithKey(HotelsTable.ID, "BUP932432")
                .column(HotelsTable.NAME)
                .column(HotelsTable.DESCRIPTION)
                .column(HotelsTable.CLASSIFICATION)
                .execute()
                .get();

        Assert.assertEquals("Budapest City", record.getString(HotelsTable.NAME));
    
        


        
        
        hotelsDao.writeWhere(QueryBuilder.in(HotelsTable.ID, "BUP932432", "BUP233544", "BUP2433"))
                 .value(HotelsTable.CLASSIFICATION, "FOUR")
                 .execute();


        record = hotelsDao.readWithKey(HotelsTable.ID, "BUP932432")
                .column(HotelsTable.NAME)
                .column(HotelsTable.DESCRIPTION)
                .column(HotelsTable.CLASSIFICATION)
                .execute()
                .get();

        Assert.assertEquals(ClassifierEnum.FOUR, record.getEnum(HotelsTable.CLASSIFICATION, ClassifierEnum.class));
        
        
        
        
        
        
        
        ////////////////
        // deletions
  
        
        try {
            hotelsDao.deleteWithKey(HotelsTable.ID,"BUP932432")
                     .onlyIf(QueryBuilder.eq(HotelsTable.NAME, "City Budapest"))
                     .execute();
            Assert.fail("IfConditionException expected");
        } catch (IfConditionException ice) { 
            System.out.println(ice.getResult().getExecutionInfo().getQueriedHost());
        }
        

        hotelsDao.deleteWithKey(HotelsTable.ID,"BUP932432")
                 .onlyIf(QueryBuilder.eq(HotelsTable.NAME, "Budapest City"))
                 .execute();

 
        
        hotelsDao.deleteWithKey(HotelsTable.ID,"BUP932432")
                 .ifExists()
                 .execute();

        
        
        
        Deletion deletion = hotelsDao.deleteWithKey("id", "BUP45544");
        hotelsDao.deleteWithKey("id", "BUP14334")
                 .combinedWith(deletion)
                 .withWriteAheadLog()
                 .execute();
        

    }
    
    
    
    @Test
    public void testWithName() throws Exception {
        
        Dao hotelsDao = new DaoImpl(cassandra.getSession(), HotelsTable.TABLE);


        hotelsDao.writeWithKey("id", "BUP3443")
                 .value(NAME, "Korona")
                 .value(ROOM_IDS, ImmutableSet.of("7", "8", "9"))
                 .value(CLASSIFICATION, ClassifierEnum.FOUR)
                 .value(ADDRESS, new Address("Sasadi út 123", "Budapest", "1112"))
                 .ifNotExists()
                 .execute();
     
        
        Record record = hotelsDao.readWithKey(ID, "BUP3443")
                                 .column(NAME)
                                 .column(CLASSIFICATION)
                                 .column(ROOM_IDS)
                                 .column(ADDRESS)
                                 .execute()
                                 .get();
        
        Assert.assertEquals("Korona", record.getValue(NAME));
        Assert.assertEquals(ClassifierEnum.FOUR, record.getValue(CLASSIFICATION));
        Assert.assertTrue(record.getValue(ROOM_IDS).contains("8"));
        Assert.assertEquals("Budapest", record.getValue(ADDRESS).getCity());

        
        
        Optional<Record> optionalRecord = hotelsDao.readWithKey(ID, "BUP3443")
                                                   .column(NAME)
                                                   .column(CLASSIFICATION)
                                                   .execute();
        optionalRecord.ifPresent(rec -> System.out.println(rec.getValue(NAME)));
        optionalRecord.ifPresent(rec -> System.out.println(rec.getValue(CLASSIFICATION)));

    }
                
}


