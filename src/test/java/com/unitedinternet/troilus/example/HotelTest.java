package com.unitedinternet.troilus.example;


import java.time.Duration;
import java.util.Iterator;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import org.junit.Assert;
import org.junit.Test;




import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.google.common.collect.ImmutableSet;
import com.unitedinternet.troilus.AbstractCassandraBasedTest;
import com.unitedinternet.troilus.Dao;
import com.unitedinternet.troilus.DaoManager;
import com.unitedinternet.troilus.Deletion;
import com.unitedinternet.troilus.Record;
import com.unitedinternet.troilus.Result;
import com.unitedinternet.troilus.reactive.MySubscriber;


public class HotelTest extends AbstractCassandraBasedTest {
    
    
    @Test
    public void testExample() throws Exception {
        DaoManager daoManager = new DaoManager(getSession());

        
        // create dao
        Dao hotelsDao = daoManager.getDao(HotelsTable.TABLE)
                                  .withConsistency(ConsistencyLevel.LOCAL_QUORUM)
                                  .withSerialConsistency(ConsistencyLevel.SERIAL);
        
        
        ////////////////
        // inserts
        hotelsDao.writeEntity(new Hotel("BUP45544", 
                                        "Corinthia Budapest",
                                        ImmutableSet.of("1", "2", "3", "122", "123", "124", "322", "333"),
                                        Optional.of(5), 
                                        Optional.of("Superb hotel housed in a heritage building - exudes old world charm")))
                 .ifNotExits()
                 .withConsistency(ConsistencyLevel.QUORUM)      
                 .withSerialConsistency(ConsistencyLevel.SERIAL)
                 .withWritetime(System.currentTimeMillis() * 10000)
                 .withTtl(Duration.ofDays(222))
                 .execute();

        hotelsDao.writeWithKey("id", "BUP932432")
                 .value("name", "City Budapest")
                 .value("room_ids", ImmutableSet.of("1", "2", "3", "4", "5"))
                 .value("classification", 4)
                 .ifNotExits()
                 .execute();

       
        // insert async
        CompletableFuture<Result> future = hotelsDao.writeEntity(new Hotel("BUP14334", 
                                                                           "Richter Panzio",
                                                                           ImmutableSet.of("1", "2", "3"),
                                                                           Optional.of(2), 
                                                                           Optional.empty()))
                                                    .withConsistency(ConsistencyLevel.ANY)
                                                    .executeAsync();
        future.get(); // waits for completion
        
        
   
        
        
        ////////////////
        // reads
        hotelsDao.readAll()
                 .asEntity(Hotel.class)
                 .withLimit(100)
                 .executeAsync()
                 .thenAccept(hotelIterator -> hotelIterator.forEachRemaining(hotel -> System.out.println(hotel)));
        
        
        
        
        MySubscriber<Hotel> mySubscriber = new MySubscriber<>();
        hotelsDao.readWhere()
                 .asEntity(Hotel.class)
                 .withLimit(100)
                 .executeAsync()
                 .thenAccept(hotels -> hotels.subscribe(mySubscriber));
        
        
        
        
        Iterator<Hotel> hotelIterator = hotelsDao.readAll()
                                                 .asEntity(Hotel.class)
                                                 .withLimit(100)
                                                 .execute();
        hotelIterator.forEachRemaining(hotel -> System.out.println(hotel));
        
        
        
        
        hotelIterator = hotelsDao.readWhere(QueryBuilder.in("ID", "BUP45544", "BUP14334"))
                                                 .asEntity(Hotel.class)
                                                 .withAllowFiltering()
                                                 .execute();
        hotelIterator.forEachRemaining(hotel -> System.out.println(hotel));
        
        

        
        
        Optional<Hotel> optionalHotel = hotelsDao.readWithKey("id", "BUP45544")
                                                 .entity(Hotel.class)
                                                 .withConsistency(ConsistencyLevel.LOCAL_ONE)
                                                 .execute();
        optionalHotel.ifPresent(hotel -> System.out.println(hotel.getName()));
        Assert.assertEquals(8, optionalHotel.get().getRoomIds().size());
        
        
       
        Hotel hotel = hotelsDao.readWithKey("id", "BUP932432")
                               .entity(Hotel.class)
                               .executeAsync()
                               .thenApply(opHotel -> opHotel.<RuntimeException>orElseThrow(RuntimeException::new))
                               .get();  // waits for completion
        System.out.println(hotel);


   
        
        
        Record record = hotelsDao.readWithKey(HotelsTable.ID, "BUP45544")
                                 .column(HotelsTable.NAME)
                                 .columnWithMetadata(HotelsTable.DESCRIPTION)
                                 .execute()
                                 .get();
        Assert.assertTrue(record.getWritetime(HotelsTable.DESCRIPTION).isPresent());
        Assert.assertFalse(record.getWritetime(HotelsTable.NAME).isPresent());
        Assert.assertTrue(record.getTtl(HotelsTable.DESCRIPTION).isPresent());
        Assert.assertFalse(record.getTtl(HotelsTable.NAME).isPresent());
        
        
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
        Assert.assertEquals("City Budapest", record.getString(HotelsTable.NAME).get());
        Assert.assertEquals("The City Budapest is in the business district on the Pest side of the river.", record.getString(HotelsTable.DESCRIPTION).get());
               
        
        
        hotelsDao.writeWithKey(HotelsTable.ID,"BUP932432")
                 .value(HotelsTable.DESCRIPTION, null)
                 .execute();
        

        record = hotelsDao.readWithKey(HotelsTable.ID, "BUP932432")
                          .column(HotelsTable.NAME)
                          .column(HotelsTable.DESCRIPTION)
                          .column(HotelsTable.CLASSIFICATION)
                          .execute()
                          .get();
        
        Assert.assertEquals((Integer) 4, record.getInt(HotelsTable.CLASSIFICATION).get());
        Assert.assertEquals("City Budapest", record.getString(HotelsTable.NAME).get());
        Assert.assertFalse(record.getString(HotelsTable.DESCRIPTION).isPresent());
        
        
        hotelsDao.writeWithKey(HotelsTable.ID, "BUP932432")
                 .value(HotelsTable.CLASSIFICATION, 5)
                 .onlyIf(QueryBuilder.eq(HotelsTable.CLASSIFICATION, 4))
                 .withSerialConsistency(ConsistencyLevel.SERIAL)
                 .execute();


        record = hotelsDao.readWithKey(HotelsTable.ID, "BUP932432")
                .column(HotelsTable.NAME)
                .column(HotelsTable.DESCRIPTION)
                .column(HotelsTable.CLASSIFICATION)
                .execute()
                .get();

        Assert.assertEquals((Integer) 5, record.getInt(HotelsTable.CLASSIFICATION).get());
    
        


        
        
        hotelsDao.writeWhere(QueryBuilder.in(HotelsTable.ID, "BUP932432", "BUP233544", "BUP2433"))
                 .value(HotelsTable.CLASSIFICATION, 4)
                 .execute();


        record = hotelsDao.readWithKey(HotelsTable.ID, "BUP932432")
                .column(HotelsTable.NAME)
                .column(HotelsTable.DESCRIPTION)
                .column(HotelsTable.CLASSIFICATION)
                .execute()
                .get();

        Assert.assertEquals((Integer) 4, record.getInt(HotelsTable.CLASSIFICATION).get());
        
        
        
        
        
        
        
        ////////////////
        // deletions
        Deletion delition = hotelsDao.deleteWithKey("id", "BUP932432");
        
        
        hotelsDao.deleteWithKey("id", "BUP14334")
                 .combinedWith(delition)
                 .withLockedBatchType()
                 .execute();
        
        
        
    }        
}


