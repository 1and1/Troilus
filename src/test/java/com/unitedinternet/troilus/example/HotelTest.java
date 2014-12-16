package com.unitedinternet.troilus.example;


import java.util.Iterator;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import org.junit.Test;

import com.datastax.driver.core.ConsistencyLevel;
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
        Dao hotelsDao = daoManager.getDao(HotelTable.TABLE)
                                  .withConsistency(ConsistencyLevel.QUORUM);
        
        
        
        // insert sync
        hotelsDao.insertObject(new Hotel("BUP45544", "Corinthia Budapest", Optional.of(5), Optional.of("Superb hotel housed in a heritage building - exudes old world charm")))
                 .withConsistency(ConsistencyLevel.ANY)          
                 .execute();

        hotelsDao.insert()
                 .value("id", "BUP932432")
                 .value("name", "City Budapest")
                 .value("classification", 4)
                 .execute();

        
        
        // insert async
        CompletableFuture<Void> future = hotelsDao.insert()
                                                  .entity(new Hotel("BUP14334", "Richter Panzio", Optional.of(2), Optional.empty()))
                                                  .withConsistency(ConsistencyLevel.ANY)
                                                  .executeAsync();

        future.get(); // waits for completion
        
        
   
        
        
        hotelsDao.read()
                 .entity(Hotel.class)
                 .withLimit(100)
                 .executeAsync()
                 .thenAccept(hotelIterator -> hotelIterator.forEachRemaining(hotel -> System.out.println(hotel)));
        
        
        
        
        MySubscriber<Hotel> mySubscriber = new MySubscriber<>();
        hotelsDao.read()
                 .entity(Hotel.class)
                 .withLimit(100)
                 .executeAsync()
                 .thenAccept(hotels -> hotels.subscribe(mySubscriber));
        
        
        Iterator<Hotel> hotelIterator = hotelsDao.read()
                                                 .entity(Hotel.class)
                                                 .withLimit(100)
                                                 .execute();
        hotelIterator.forEachRemaining(hotel -> System.out.println(hotel));
        
        
        
        
        

        
        
        // read sync
        Optional<Hotel> optionalHotel = hotelsDao.readWithKey("id", "BUP45544")
                                                 .entity(Hotel.class)
                                                 .withConsistency(ConsistencyLevel.ONE)
                                                 .execute();
        optionalHotel.ifPresent(hotel -> System.out.println(hotel.getName()));
        
        
        Optional<Record> optionalRecord = hotelsDao.readWithKey("id", "BUP14334")
                                                   .column("id")
                                                   .column("name")
                                                   .execute();
        optionalRecord.ifPresent(record -> record.getString("name").ifPresent(name -> System.out.println(name)));
        
        
        // read async
        Hotel hotel = hotelsDao.readWithKey("id", "BUP932432")
                               .entity(Hotel.class)
                               .executeAsync()
                               .thenApply(opHotel -> opHotel.<RuntimeException>orElseThrow(RuntimeException::new))
                               .get();  // waits for completion
        System.out.println(hotel);

        
   
        
        
        
        Deletion delition = hotelsDao.deleteWithKey("id", "BUP932432");
        
        
        hotelsDao.deleteWithKey("id", "BUP14334")
                 .combinedWith(delition)
                 .withLockedBatchType()
                 .execute();
    }        
}


