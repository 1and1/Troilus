package net.oneandone.troilus.example;


import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import net.oneandone.troilus.AbstractCassandraBasedTest;
import net.oneandone.troilus.Dao;
import net.oneandone.troilus.DaoImpl;
import net.oneandone.troilus.IfConditionException;
import net.oneandone.troilus.Record;
import net.oneandone.troilus.Result;
import net.oneandone.troilus.Dao.Batchable;
import net.oneandone.troilus.reactive.MySubscriber;

import org.junit.Assert;
import org.junit.Test;















import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.google.common.collect.ImmutableSet;

import static net.oneandone.troilus.example.HotelTableFields.*;


public class HotelTest extends AbstractCassandraBasedTest {
    
    
    @Test
    public void testExample() throws Exception {
                
        // create dao
        Dao hotelsDao = new DaoImpl(getSession(), HotelsTable.TABLE)
                                  .withConsistency(ConsistencyLevel.LOCAL_QUORUM)
                                  .withInterceptor(HotelsTable.CONSTRAINTS);
        
        
        ////////////////
        // inserts
        hotelsDao.writeEntity(new Hotel("BUP45544", 
                                        "Corinthia Budapest",
                                        ImmutableSet.of("1", "2", "3", "122", "123", "124", "322", "333"),
                                        Optional.of(ClassifierEnum.FIVE), 
                                        Optional.of("Superb hotel housed in a heritage building - exudes old world charm"),
                                        new Address("Erzsébet körút 43", "Budapest", "1073"))
                                       )
                 .ifNotExists()
                 .withConsistency(ConsistencyLevel.QUORUM)      
                 .withSerialConsistency(ConsistencyLevel.SERIAL)
                 .withWritetime(System.currentTimeMillis() * 10000)
                 .withTtl(Duration.ofDays(222))
                 .execute();

        
        
        Record record = hotelsDao.readWithKey(HotelsTable.ID, "BUP45544")
                                 .column(HotelsTable.NAME)
                                 .columnWithMetadata(HotelsTable.DESCRIPTION)
                                 .execute()
                                 .get();
        Assert.assertTrue(record.getWritetime(HotelsTable.DESCRIPTION).isPresent());
        Assert.assertFalse(record.getWritetime(HotelsTable.NAME).isPresent());
        Assert.assertTrue(record.getTtl(HotelsTable.DESCRIPTION).isPresent());
        Assert.assertFalse(record.getTtl(HotelsTable.NAME).isPresent());
        
        
        
      
        
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
        
        
        
        
        MySubscriber<Hotel> mySubscriber = new MySubscriber<>();
        hotelsDao.readWhere()
                 .asEntity(Hotel.class)
                 .withLimit(100)
                 .executeAsync()
                 .thenAccept(hotels -> hotels.subscribe(mySubscriber));
        
        
        
        
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
        
        Assert.assertNotNull(record.getObject("address", Address.class));
        Assert.assertEquals("TWO",  record.getString("classification").get());
        Assert.assertEquals("Budapest", record.getObject("address", Address.class).get().getCity());


        
        
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
        Assert.assertEquals("City Budapest", record.getString(HotelsTable.NAME).get());
        Assert.assertEquals("The City Budapest is in the business district on the Pest side of the river.", record.getString(HotelsTable.DESCRIPTION).get());
               
        
        
        hotelsDao.writeWithKey(HotelsTable.ID,"BUP932432")
                 .value(HotelsTable.DESCRIPTION, Optional.empty())
                 .execute();
        

        record = hotelsDao.readWithKey(HotelsTable.ID, "BUP932432")
                          .column(HotelsTable.NAME)
                          .column(HotelsTable.DESCRIPTION)
                          .column(HotelsTable.CLASSIFICATION)
                          .execute()
                          .get();
        
        Assert.assertEquals(ClassifierEnum.FOUR.toString(), record.getString(HotelsTable.CLASSIFICATION).get());
        Assert.assertEquals("City Budapest", record.getString(HotelsTable.NAME).get());
        Assert.assertFalse(record.getString(HotelsTable.DESCRIPTION).isPresent());
        
        
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

        Assert.assertEquals("Budapest City", record.getString(HotelsTable.NAME).get());
    
        


        
        
        hotelsDao.writeWhere(QueryBuilder.in(HotelsTable.ID, "BUP932432", "BUP233544", "BUP2433"))
                 .value(HotelsTable.CLASSIFICATION, "FOUR")
                 .execute();


        record = hotelsDao.readWithKey(HotelsTable.ID, "BUP932432")
                .column(HotelsTable.NAME)
                .column(HotelsTable.DESCRIPTION)
                .column(HotelsTable.CLASSIFICATION)
                .execute()
                .get();

        Assert.assertEquals(ClassifierEnum.FOUR, record.getEnum(HotelsTable.CLASSIFICATION, ClassifierEnum.class).get());
        
        
        
        
        
        
        
        ////////////////
        // deletions
  
        
        try {
            hotelsDao.deleteWithKey(HotelsTable.ID,"BUP932432")
                     .onlyIf(QueryBuilder.eq(HotelsTable.NAME, "City Budapest"))
                     .execute();
            Assert.fail("IfConditionException expected");
        } catch (IfConditionException expected) { }
        

        hotelsDao.deleteWithKey(HotelsTable.ID,"BUP932432")
                 .onlyIf(QueryBuilder.eq(HotelsTable.NAME, "Budapest City"))
                 .execute();

 
        
        hotelsDao.deleteWithKey(HotelsTable.ID,"BUP932432")
                 .ifExists()
                 .execute();

        
        
        
        Batchable deletion = hotelsDao.deleteWithKey("id", "BUP45544");
        hotelsDao.deleteWithKey("id", "BUP14334")
                 .combinedWith(deletion)
                 .withWriteAheadLog()
                 .execute();
        

    }
    
    
    
    @Test
    public void testWithName() throws Exception {
        
        Dao hotelsDao = new DaoImpl(getSession(), HotelsTable.TABLE);


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
        
        Assert.assertEquals("Korona", record.getValue(NAME).get());
        Assert.assertEquals(ClassifierEnum.FOUR, record.getValue(CLASSIFICATION).get());
        Assert.assertTrue(record.getValue(ROOM_IDS).get().contains("8"));
        Assert.assertEquals("Budapest", record.getValue(ADDRESS).get().getCity());

        
        
        Optional<Record> optionalRecord = hotelsDao.readWithKey(ID, "BUP3443")
                                                   .column(NAME)
                                                   .column(CLASSIFICATION)
                                                   .execute();
        optionalRecord.ifPresent(rec -> rec.getValue(NAME).ifPresent(name -> System.out.println(name)));
        optionalRecord.ifPresent(rec -> rec.getValue(CLASSIFICATION).ifPresent(classification -> System.out.println(classification)));

    }
                
}


