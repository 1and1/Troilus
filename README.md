
Troilus
======
**Troilus** is a high level Cassandra Java8 client on the top of the [DataStax Java Driver for Apache Cassandra](https://github.com/datastax/java-driver). 
It supports synchronous programming as well as asynchronous programming including the [reactive streams](http://www.reactive-streams.org) interface.


#Examples
-------

##Create a Dao
First a DataStax Java Driver [Session](https://github.com/datastax/java-driver) object has to be created
``` java
Cluster cluster = Cluster.builder()
                         .addContactPoint(node)
                         .build();
Session session = cluster.connect("hotel_reservation_system");
```

This Session object will be used to create the DaoManager and fetching Dao's based on the table name. In the examples below the [hotels table](src/test/resources/com/unitedinternet/troilus/example/hotels.ddl) is used
``` java
DaoManager daoManager = new DaoManager(session);
Dao hotelDao = daoManager.getDao("hotels")
                         .withConsistency(ConsistencyLevel.QUORUM);
```

##Insert
Insert a row in a column-oriented way
``` java
hotelDao.insert()
        .value("id", "BUP932432")
        .value("name", "City Budapest")
        .value("classification", 4)
        .execute();
```


Insert a row in an entity-oriented way.  
``` java
hotelDao.insert()
        .entity(new Hotel("BUP14334", "Richter Panzio", Optional.of(2), Optional.empty()))
        .ifNotExits()
        .execute();
```
The columns will be mapped by using [@Column](http://docs.oracle.com/javaee/7/api/javax/persistence/Column.html) annotated fields and setter/getter method. The annotation attribute *name* is supported only. Setting a  @Entity or @Table annotation is not necessary and will be ignored
``` java
import java.util.Optional;
import javax.persistence.Column;

public class Hotel  {
   
    @Column(name = "id")
    private String id = null;
    
    @Column(name = "name")
    private String name = null;
    
    @Column(name = "classification")
    private Optional<Integer> classification = Optional.empty();
    
    @Column(name = "description")
    private Optional<String> description = Optional.empty();

    
    private Hotel() {  }  // empty constructor is required for deserializing purposes
    
    public Hotel(String id, String name, Optional<Integer> classification, Optional<String> description) {
        this.id = id;
        this.name = name;
        this.classification = classification;
        this.description = description;
    }

    public String getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public Optional<Integer> getClassification() {
        return classification;
    }

    public Optional<String> getDescription() {
        return description;
    }  
}
```



##Delete

``` java
hotelsDao.deleteWithKey("id", "BUP932432")
         .execute();
```


##Batching        
Non conditional mutating operations (insert, update, delete) can be executed in a batched manner by combining it with another mutating operation
``` java
Deletion deletion = hotelsDao.deleteWithKey("id", "BUP932432");

hotelsDao.deleteWithKey("id", "BUP14334")
         .combinedWith(deletion)
         .withLockedBatchType()
         .execute();
```


##Read
###Read a single row

Read a row in an entity-oriented way.  
``` java        
Optional<Hotel> optionalHotel = hotelsDao.readWithKey("id", "BUP45544")
                                         .entity(Hotel.class)
                                         .execute();
optionalHotel.ifPresent(hotel -> System.out.println(hotel.getName()));
```        

Read a row in a column-oriented way
``` java        
Optional<Record> optionalRecord = hotelsDao.readWithKey("id", "BUP14334")
                                           .column("id")
                                           .column("name")
                                           .withConsistency(ConsistencyLevel.ONE)
                                           .execute();
optionalRecord.ifPresent(record -> record.getString("name").ifPresent(name -> System.out.println(name)));
```        

###Read a list of rows

Read all of the table
``` java  
Iterator<Hotel> hotelIterator = hotelsDao.readAll()
                                         .entity(Hotel.class)
                                         .withLimit(5000)
                                         .execute();
hotelIterator.forEachRemaining(hotel -> System.out.println(hotel));
```        
        

Read specific ones by using conditions
``` java  
Iterator<Hotel> hotelIterator = hotelsDao.readWithCondition(QueryBuilder.in("ID", "BUP45544", "BUP14334"))
                                         .entity(Hotel.class)
                                         .withAllowFiltering()
                                         .execute();
hotelIterator.forEachRemaining(hotel -> System.out.println(hotel));                
```        
        

        
#Asynchronous Examples
-------

##Async Insert
By calling ***executeAsync()*** instead *execute()* the method returns immediately without waiting for the database response. Further more the executeAsync() returns a Java8 [CompletableFuture](https://docs.oracle.com/javase/8/docs/api/java/util/concurrent/CompletableFuture.html) object which can be used for async processing
``` java
CompletableFuture<Void> future = hotelsDao.insert()
                                          .entity(new Hotel("BUP14334", "Richter Panzio", Optional.of(2), Optional.empty()))
                                          .withConsistency(ConsistencyLevel.ANY)
                                          .executeAsync();
```


##Async Read

As already mentioned above the methods returns immediately without waiting for the database response. The consumer code within the thenAccept(...) method will be called as soon as the database response is received. However, the Iterator has still a blocking behavior.
``` java
hotelsDao.readAll()
         .entity(Hotel.class)
         .withLimit(5000)
         .executeAsync()
         .thenAccept(hotelIterator -> hotelIterator.forEachRemaining(hotel -> System.out.println(hotel)));
```

For true asynchronous streaming a [Subscriber](http://www.reactive-streams.org) could be registered which will be executed in an asynchronous, reactive way
``` java
Subscriber<Hotel> mySubscriber = new MySubscriber();  

hotelsDao.readAll()
         .entity(Hotel.class)
         .withLimit(5000)
         .executeAsync()
         .thenAccept(publisher -> publisher.subscribe(mySubscriber));
```

The Subscriber implements call back methods such as onNext(...) or onError(...) to process the result stream in a reactive way. By calling the hotels.subscribe(subscriber) above the onSubscribe(...) method of the subscriber below will be called.
``` java
import java.util.concurrent.atomic.AtomicReference;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

public class MySubscriber<T> implements Subscriber<Hotel> {
    private final AtomicReference<Subscription> subscriptionRef = new AtomicReference<>();
    //...
    
    @Override
    public void onSubscribe(Subscription subscription) {
        this.subscriptionRef.set(subscription);
        subscription.request(1);  // here, requesting elements starts the streaming implicitly
    }
    
    @Override
    public void onNext(Hotel hotel) {
        System.out.println(hotel);
        subscription.request(1);
    }
    
    @Override
    public void onComplete() {
        //..
    }
    
    @Override
    public void onError(Throwable t) {
        //..
    }
}
```
