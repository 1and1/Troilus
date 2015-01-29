
Troilus
======
**Troilus** is a high level Cassandra Java client on the top of the [DataStax Java Driver for Apache Cassandra](https://github.com/datastax/java-driver). 
It supports synchronous programming and asynchronous programming.

The main features of Troilus are 
* providing a Java8-based Interface as well as a Java7-based interface (module troilus-core-java7)
* Supporting sync as well as async programming
* [reactive streams](http://www.reactive-streams.org) support
* (Entity) Bean-Mapping support for tables and user defined data types (incl. mapping support of generic artefacts such as Java8/Guava Optional and Guava ImmutableCollections)
* Build-in data swap check 
* Build-in prepared statement management 
* Implementation support for data-related constraints on the app-level (mandatory fields, more complex data swap validation checks, …)          

#Maven
-------
Java8-based
``` java
<dependency>
	<groupId>net.oneandone</groupId>
	<artifactId>troilus-core</artifactId>
	<version>0.3</version>
</dependency>
```


Java7-based
``` java
<dependency>
	<groupId>net.oneandone</groupId>
	<artifactId>troilus-core-java7</artifactId>
	<version>0.3</version>
</dependency>
```


#Examples
-------

##Create a Dao
First a DataStax Java Driver [Session](https://github.com/datastax/java-driver) object has to be created
``` java
Cluster cluster = Cluster.builder()
                         .addContactPoint(node)
                         .build();
Session session = cluster.connect("ks_hotel_reservation_system");
```



This Session object will be used to create a new instance of a `Dao`. In the examples below the [hotels table](troilus-core/src/test/resources/com/unitedinternet/troilus/example/hotels.ddl) is used
``` java
Dao hotelsDao = new DaoImpl(session, "hotels");
```

Pre-configured dao
``` java
Dao hotelsDao = new DaoImpl(session, "hotels")
                          .withConsistency(ConsistencyLevel.LOCAL_QUORUM);
```

##Write
Write a row in a column-oriented way
``` java
hotelsDao.writeWithKey("id", "BUP932432")
         .value("name", "City Budapest")
         .value("room_ids", ImmutableSet.of("1", "2", "3", "122", "123", "124", "322", "333"))
         .value("classification", ClassifierEnum.FOUR)
         .withWritetime(microsSinceEpoch)
         .execute();
```


Write a row in an entity-oriented way.  
``` java
hotel = new Hotel("BUP14334", 
                  "Richter Panzio",
       	          ImmutableSet.of("1", "2", "3", "4", "5"),
                  Optional.of(ClassifierEnum.TWO),
                  Optional.empty());

hotelsDao.writeEntity(hotel)
         .execute();
```

The columns will be mapped by using `@Field` annotated fields. The JEE [`@Column`](http://docs.oracle.com/javaee/7/api/javax/persistence/Column.html) annotation is also supported for compatibility reasons. However, the name field is supported only  
``` java
public class Hotel  {
   
    @Field(name = "id")
    private String id = null;
    
    @Field(name = "name")
    private String name = null;

    @Field(name = "room_ids")
    private ImmutableSet<String> roomIds = ImmutableSet.of();

    @Field(name = "classification")
    private Optional<ClassifierEnum> classification = Optional.empty();
    
    @Field(name = "description")
    private Optional<String> description = Optional.empty();
        
    
    @SuppressWarnings("unused")
    private Hotel() { }
    
    public Hotel(String id, 
                 String name, 
                 ImmutableSet<String> roomIds,  
                 Optional<ClassifierEnum> classification, 
                 Optional<String> description) {
        this.id = id;
        this.name = name;
        this.roomIds = roomIds;
        this.classification = classification;
        this.description = description;
    }

    public String getId() {
        return id;
    }

    public String getName() {
        return name;
    }
    
    public ImmutableSet<String> getRoomIds() {
        return roomIds;
    }

    public Optional<ClassifierEnum> getClassification() {
        return classification;
    }

    public Optional<String> getDescription() {
        return description;
    }
}
```


### updating values
``` java
hotelsDao.writeWithKey("id","BUP932432")
         .value("description", "The City Budapest is in the business district on the Pest side of the river.")
         .execute();
  ```               

### removing values
``` java
hotelsDao.writeWithKey("id","BUP932432")
         .value("description", Optional.empty())  
         .execute();
```             

or
``` java
hotelsDao.writeWithKey("id","BUP932432")
         .value("description", null)  
         .execute();
```  


### value update based on where conditions
``` java
hotelsDao.writeWhere(QueryBuilder.in("id", "BUP932432", "BUP233544", "BUP2433"))
         .value("classification", ClassifierEnum.FOUR)
         .execute();
```               
                
        
##Delete

``` java
hotelsDao.deleteWithKey("id", "BUP932432")
         .execute();
```

        

### lightweight transactions 
transaction-safe, ***unique insert*** with `ifNotExists()`(performs the insertion only if the row does not already exist)        
``` java
try {
   hotelsDao.writeWithKey("id", "BUP932432")
            .value("name", "City Budapest")
            .value("room_ids", ImmutableSet.of("1", "2", "3", "122", "123", "124", "322", "333"))
            .value("classification", ClassifierEnum.FOUR)
            .withWritetime(microsSinceEpoch)
            .ifNotExists()
            .withSerialConsistency(ConsistencyLevel.SERIAL)
            .execute();
         
} catch (IfConditionException ice) {
   // ...
}
```  
        
transaction-safe, ***conditional update*** with `onlyIf(..conditions..)` (uses IF followed by a condition to be met for the update to succeed)        
``` java
try {
   hotelsDao.writeWithKey("id", "BUP932432")
            .value("name" "Budapest City")
            .onlyIf(QueryBuilder.eq("name", "City Budapest"))
            .withSerialConsistency(ConsistencyLevel.SERIAL)
            .execute();
                                 
} catch (IfConditionException ice) {
   // ...
}
```  
       

transaction-safe, ***conditional delete*** with `onlyIf(..conditions..)` (uses IF followed by a condition to be met for the update to succeed)        
``` java
try {
   hotelsDao.deleteWithKey("id","BUP932432")
            .onlyIf(QueryBuilder.eq("name", "Budapest City"))
            .withSerialConsistency(ConsistencyLevel.SERIAL)
            .execute();
                                
} catch (IfConditionException ice) {
   // ...
}         
```  
  
  
transaction-safe ***delete*** with `ifExists` 
``` java
try {
   hotelsDao.deleteWithKey("id","BUP932432")
            .ifExists()
            .withSerialConsistency(ConsistencyLevel.SERIAL)
            .execute();
                                
} catch (IfConditionException ice) {
   // ...
}         
```  



##Batching        
Non conditional mutating operations (insert, update, delete) can be executed in a batched manner by combining it with another mutating operation. This is provided by the `combinedWith(...)` method. 
``` java
Batchable deletion = hotelsDao.deleteWithKey("id", "BUP932432");

hotelsDao.deleteWithKey("id", "BUP14334")
         .combinedWith(deletion)
         .withWriteAheadLog()
         .execute();
```


##Read
###Read a single row

Read a row in an entity-oriented way.  
``` java        
Optional<Hotel> optionalHotel = hotelsDao.readWithKey("id", "BUP45544")
                                         .asEntity(Hotel.class)
                                         .execute();
optionalHotel.ifPresent(hotel -> System.out.println(hotel.getName()));
```        

Read a row in a column-oriented way
``` java        
Optional<Record> optionalRecord = hotelsDao.readWithKey("id", "BUP14334")
                                           .column("id")
                                           .column("name")
                                           .withConsistency(ConsistencyLevel.LOCAL_ONE)
                                           .execute();
optionalRecord.ifPresent(record -> record.getString("name").ifPresent(name -> System.out.println(name)));
```        

Read a row in a column-oriented way with `Name` definitions. 
``` java        
import static ....HotelTableColumns.*;

Optional<Record> optionalRecord = hotelsDao.readWithKey(ID, "BUP3443")
                                           .column(NAME)
                                           .column(CLASSIFICATION)
                                           .execute();
optionalRecord.ifPresent(record -> record.getValue(NAME).

Present(name -> System.out.println(name)));
optionalRecord.ifPresent(record -> record.getValue(CLASSIFICATION).ifPresent(classification -> System.out.println(classification)));
```        


with definitions
``` java        
public final class HotelTableColumns  {
    public static final ColumnName<String> ID = ColumnName.defineString("id");
    public static final ColumnName<String> NAME = ColumnName.defineString("name");
    public static final ColumnName<Set<String>> ROOM_IDS = ColumnName.defineSet("room_ids", String.class);
    public static final ColumnName<Address> ADDRESS = ColumnName.define("address", Address.class);
    public static final ColumnName<String> DESCRIPTION = ColumnName.defineString("description");
    public static final ColumnName<ClassifierEnum> CLASSIFICATION = ColumnName.define("classification", ClassifierEnum.class);
}
```        



Read with meta data (ttl, writetime)
``` java        
Record record = hotelsDao.readWithKey("id", "BUP14334")
                         .column("id")
         	             .column("name")
            	         .columnWithMetadata("description")
                         .withConsistency(ConsistencyLevel.LOCAL_ONE)
                         .execute()
                         .get();
                                           
record.getTtl("description").ifPresent(ttl -> System.out.println("ttl=" + ttl)));
```        

  

###Read a list of rows

Read all of the table
``` java  
Iterable<Hotel> hotelIterator = hotelsDao.readAll()
                                         .asEntity(Hotel.class)
                                         .withLimit(5000)
                                         .execute();
hotelIterator.forEach(hotel -> System.out.println(hotel));
```        
        

Read specific ones by using conditions
``` java  
Iterable<Hotel> hotelIterator = hotelsDao.readWhere(QueryBuilder.in("ID", "BUP45544", "BUP14334"))
                                         .asEntity(Hotel.class)
                                         .withAllowFiltering()
                                         .execute();
hotelIterator.forEach(hotel -> System.out.println(hotel));                
```        
        

      
#User-defined types support
-------

to use the user-defined types support a Java class which represents the user-definied type has to be implemented. The fields to be mapped have to be annotated with `@Field`


``` java
public class Address {

    @Field(name = "street")
    private String street;
    
    @Field(name = "city")
    private String city;
    
    @Field(name = "post_code")
    private String postCode;
    
        
    @SuppressWarnings("unused")
    private Address() { }

    
    public Address(String street, String city, String postCode) {
        this.street = street;
        this.city = city;
        this.postCode = postCode;
    }


    public String getStreet() {
        return street;
    }


    public String getCity() {
        return city;
    }


    public String getPostCode() {
        return postCode;
    }
}
```



##Write

Write a row in a column-oriented way
``` java
hotelsDao.writeWithKey("id", "BUP932432")
         .value("name", "City Budapest")
         .value("room_ids", ImmutableSet.of("1", "2", "3", "122", "123", "124", "322", "333"))
         .value("classification", ClassifierEnum.FOUR)
         .value("address", new Address("Thököly Ut 111", "Budapest", "1145"))
         .withWritetime(microsSinceEpoch)
         .execute();
```


Write a row in a entity-oriented way
``` java
hotel = new Hotel("BUP14334", 
                  "Richter Panzio",
                  ImmutableSet.of("1", "2", "3", "4", "5"),
                  Optional.of(ClassifierEnum.TWO),
                  Optional.empty(),
                  new Address("Thököly Ut 111", "Budapest", "1145"));

hotelsDao.writeEntity(hotel)
         .execute();
```

``` java
public class Hotel  {
   
    @Field(name = "id")
    private String id = null;
    
    @Field(name = "name")
    private String name = null;

    @Field(name = "room_ids")
    private ImmutableSet<String> roomIds = ImmutableSet.of();

    @Field(name = "classification")
    private Optional<ClassifierEnum> classification = Optional.empty();
    
    @Field(name = "description")
    private Optional<String> description = Optional.empty();

    @Field(name = "address")
    private Address address = null;

        
    
    @SuppressWarnings("unused")
    private Hotel() { }
    
    public Hotel(String id, 
                 String name, 
                 ImmutableSet<String> roomIds,  
                 Optional<ClassifierEnum> classification, 
                 Optional<String> description,
                 Address address) {
        this.id = id;
        this.name = name;
        this.roomIds = roomIds;
        this.classification = classification;
        this.description = description;
        this.address = address;
    }

    public String getId() {
        return id;
    }

    public String getName() {
        return name;
    }
    
    public ImmutableSet<String> getRoomIds() {
        return roomIds;
    }

    public Optional<ClassifierEnum> getClassification() {
        return classification;
    }

    public Optional<String> getDescription() {
        return description;
    }
    
    public Address getAddress() {
        return address;
    }
}
```



##Read

Read a row in a entity-oriented way
``` java
hotel = hotelsDao.readWithKey("id", "BUP14334")
                 .asEntity(Hotel.class)
                 .execute()
                 .get();
        
System.out.println(hotel.getAddress());
```


Read a row in a column-oriented way
``` java
record = hotelsDao.readWithKey("id", "BUP14334")
                  .column("id")
                  .column("name")
                  .column("classification")
                  .column("address")
                  .withConsistency(ConsistencyLevel.LOCAL_ONE)
                  .execute()
                  .get();
        
System.out.println(record.getString("classification"));
System.out.println(record.getObject("address", Address.class));
```


        
#Asynchronous Examples
-------

##Async Write
By calling `executeAsync()` instead `execute()` the method returns immediately without waiting for the database response. Further more the `executeAsync()` returns a Java8 [CompletableFuture](https://docs.oracle.com/javase/8/docs/api/java/util/concurrent/CompletableFuture.html) object which can be used for async processing
``` java
hotel = new Hotel("BUP14334", 
                  "Richter Panzio", 
                  Optional.of(ClassifierEnum.TWO), 
                  Optional.empty());
                  
CompletableFuture<Result> future = hotelsDao.writeEntity(hotel)
                                            .withConsistency(ConsistencyLevel.ANY)
                                            .executeAsync();
```


##Async Read

As already mentioned above the methods returns immediately without waiting for the database response. The consumer code within the `thenAccept(...)` method will be called as soon as the database response is received. 

read single row
``` java
hotelsDao.readWithKey("id", "BUP45544")
         .asEntity(Hotel.class)
	     .executeAsync()
         .thenAccept(optionalHotel.ifPresent(hotel -> System.out.println(hotel));
```


read a list of rows. Please consider that the Iterator has a blocking behavior which means the streaming of the result could block
``` java
hotelsDao.readAll()
         .asEntity(Hotel.class)
         .withLimit(5000)
         .executeAsync()
         .thenAccept(hotelIt -> hotelIt.forEach(hotel -> System.out.println(hotel)));
```

For true asynchronous streaming a [Subscriber](http://www.reactive-streams.org) could be registered which will be executed in an asynchronous, reactive way
``` java
Subscriber<Hotel> mySubscriber = new MySubscriber();  

hotelsDao.readAll()
         .asEntity(Hotel.class)
         .withLimit(5000)
         .executeAsync()
         .thenAccept(publisher -> publisher.subscribe(mySubscriber));
```

The Subscriber implements call back methods such as `onNext(...)` or `onError(...)` to process the result stream in a reactive way. By calling the hotels.subscribe(subscriber) above the `onSubscribe(...)` method of the subscriber below will be called.
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




#Interceptor Examples
The interceptor support can be used to implement (more complex) constraint checks on the client-side  (Cassandra also supports server-side [trigger](http://www.datastax.com/documentation/cql/3.1/cql/cql_reference/trigger_r.html) which can also be used to implement contraints). To register interceptors the `Dao` supports the `withInterceptor(...)` method.

``` java
Dao phoneNumbersDao = new DaoImpl(getSession(), "phone_numbers");
       
Dao phoneNumbersDaoWithConstraints = phoneNumbersDao.withInterceptor(new PhonenumbersConstraints(deviceDao))
												    .withInterceptor(ConstraintsInterceptor.newConstraints()
                                                                                           .withNotNullColumn("device_id")
                                                                                           .withImmutableColumn("device_id"));
```

##ConstraintsInterceptor Examples
To implement simple constraints the  `ConstraintsInterceptor` can be used 
``` java
Dao phoneNumbersDaoWithConstraints = phoneNumbersDao.withInterceptor(ConstraintsInterceptor.newConstraints()
                                                                                           .withNotNullColumn("device_id")
                                                                                           .withImmutableColumn("device_id"));
```

##More Complexe Interceptor Examples
The interceptor below implements a back relation check regarding the [phone_numbers](troilus-core/src/test/resources/com/unitedinternet/troilus/example/phone_numbers.ddl) table. 
``` java
class PhonenumbersConstraints implements SingleReadQueryRequestInterceptor,
                                         SingleReadQueryResponseInterceptor {
    

    private final Dao deviceDao;
    
    public PhonenumbersConstraints(Dao deviceDao) {
        this.deviceDao = deviceDao.withConsistency(ConsistencyLevel.QUORUM);
    }
        
    
    @Override
    public CompletableFuture<SingleReadQueryData> onSingleReadRequestAsync( SingleReadQueryData queryData) {
        // force that device_id will be fetched 
        if (!queryData.getColumnsToFetch().isEmpty() && !queryData.getColumnsToFetch().containsKey("device_id")) {
            queryData = queryData.columnsToFetch(Immutables.merge(queryData.getColumnsToFetch(), "device_id", false));
        }
        return CompletableFuture.completedFuture(queryData);
    }
    
    
    @Override
    public CompletableFuture<Optional<Record>> onSingleReadResponseAsync(SingleReadQueryData queryData, Optional<Record> optionalRecord) {

        if (optionalRecord.isPresent() && optionalRecord.get().getString("device_id").isPresent()) {
            return deviceDao.readWithKey("device_id", optionalRecord.get().getString("device_id").get())
                            .column("phone_numbers")
                            .withConsistency(ConsistencyLevel.ONE)
                            .executeAsync()
                            .thenApply(optionalRec -> {
                                                        optionalRec.ifPresent(rec -> {
                                                            Optional<ImmutableSet<String>> set = rec.getSet("phone_numbers", String.class);
                                                            if (set.isPresent() && !set.get().contains(queryData.getKey().get("number"))) {
                                                                throw new ConstraintException("reverse reference devices table -> phone_numbers table does not exit");
                                                            }
                                                        });
                                                        
                                                        return optionalRecord;
                                                      });
            
        } else {
            return CompletableFuture.completedFuture(optionalRecord);
        }
    }    
}
```
