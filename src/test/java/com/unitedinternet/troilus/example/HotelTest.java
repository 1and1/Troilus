package com.unitedinternet.troilus.example;


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
import com.unitedinternet.troilus.reactive.MySubscriber;


public class HotelTest extends AbstractCassandraBasedTest {
    
    
    @Test
    public void testExample() throws Exception {
        DaoManager daoManager = new DaoManager(getSession());

        
        // create dao
        Dao hotelsDao = daoManager.getDao(HotelsTable.TABLE)
                                  .withConsistency(ConsistencyLevel.LOCAL_QUORUM);
        
        
        ////////////////
        // inserts
        hotelsDao.write()
                 .entity(new Hotel("BUP45544", 
                                   "Corinthia Budapest",
                                   ImmutableSet.of("1", "2", "3", "122", "123", "124", "322", "333"),
                                   Optional.of(5), 
                                   Optional.of("Superb hotel housed in a heritage building - exudes old world charm")))
                 .ifNotExits()
                 .withConsistency(ConsistencyLevel.QUORUM)      
                 .withSerialConsistency(ConsistencyLevel.SERIAL)
                 .execute();

        hotelsDao.write()
                 .value("id", "BUP932432")
                 .value("name", "City Budapest")
                 .value("room_ids", ImmutableSet.of("1", "2", "3", "4", "5"))
                 .value("classification", 4)
                 .execute();

       
        // insert async
        CompletableFuture<Void> future = hotelsDao.write()
                                                  .entity(new Hotel("BUP14334", 
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
                 .entity(Hotel.class)
                 .withLimit(100)
                 .executeAsync()
                 .thenAccept(hotelIterator -> hotelIterator.forEachRemaining(hotel -> System.out.println(hotel)));
        
        
        
        
        MySubscriber<Hotel> mySubscriber = new MySubscriber<>();
        hotelsDao.readWithCondition()
                 .entity(Hotel.class)
                 .withLimit(100)
                 .executeAsync()
                 .thenAccept(hotels -> hotels.subscribe(mySubscriber));
        
        
        
        
        Iterator<Hotel> hotelIterator = hotelsDao.readAll()
                                                 .entity(Hotel.class)
                                                 .withLimit(100)
                                                 .execute();
        hotelIterator.forEachRemaining(hotel -> System.out.println(hotel));
        
        
        
        
        hotelIterator = hotelsDao.readWithCondition(QueryBuilder.in("ID", "BUP45544", "BUP14334"))
                                                 .entity(Hotel.class)
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


   
        
        
        Record record = hotelsDao.readWithKey(HotelsTable.ID, "BUP932432")
                                 .column(HotelsTable.NAME)
                                 .column(HotelsTable.DESCRIPTION)
                                 .execute()
                                 .get();
        Assert.assertEquals("City Budapest", record.getString(HotelsTable.NAME).get());
        Assert.assertFalse(record.getString(HotelsTable.DESCRIPTION).isPresent());
        
        
        ////////////////
        // updates
        
        hotelsDao.write()
                 .value(HotelsTable.ID,"BUP932432")
                 .value(HotelsTable.DESCRIPTION, "The City Budapest is in the business district on the Pest side of the river.")
                 .execute();
                 
        
        record = hotelsDao.readWithKey(HotelsTable.ID, "BUP932432")
                          .column(HotelsTable.NAME)
                          .column(HotelsTable.DESCRIPTION)
                          .execute()
                          .get();
        Assert.assertEquals("City Budapest", record.getString(HotelsTable.NAME).get());
        Assert.assertEquals("The City Budapest is in the business district on the Pest side of the river.", record.getString(HotelsTable.DESCRIPTION).get());
               
        
        
        hotelsDao.write()
                 .value(HotelsTable.ID,"BUP932432")
                 .value(HotelsTable.DESCRIPTION, null)
                 .execute();
        

        record = hotelsDao.readWithKey(HotelsTable.ID, "BUP932432")
                          .column(HotelsTable.NAME)
                          .column(HotelsTable.DESCRIPTION)
                          .execute()
                          .get();
        Assert.assertEquals("City Budapest", record.getString(HotelsTable.NAME).get());
        Assert.assertFalse(record.getString(HotelsTable.DESCRIPTION).isPresent());

        
        
        
        
        ////////////////
        // deletions
        Deletion delition = hotelsDao.deleteWithKey("id", "BUP932432");
        
        
        hotelsDao.deleteWithKey("id", "BUP14334")
                 .combinedWith(delition)
                 .withLockedBatchType()
                 .execute();
        
        
        
    }        
}


