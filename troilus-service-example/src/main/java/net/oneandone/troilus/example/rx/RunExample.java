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
package net.oneandone.troilus.example.rx;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.Random;

import net.oneandone.troilus.CassandraDB;
import net.oneandone.troilus.Dao;
import net.oneandone.troilus.DaoImpl;

import com.datastax.driver.core.Session;
import com.google.common.collect.ImmutableList;

import rx.Observable;
import rx.RxReactiveStreams;

 

public class RunExample {
    
    public static void main(String[] args) throws InterruptedException, IOException {
        
        try (CassandraDB cassandra = CassandraDB.newInstance()) {

            // create schema
            cassandra.executeCql("CREATE TABLE hotelrating (epoch_day bigInt, " + 
                                 "                          epoch_millis bigInt, " +
                                 "                          hotel_id text, " + 
                                 "                          user_id text, " + 
                                 "                          rating int, " +
                                 "                          PRIMARY KEY (epoch_day, epoch_millis)) " +
                                 "                          WITH CLUSTERING ORDER BY (epoch_millis DESC);");
            Session session = cassandra.getSession();
            Dao ratingsDao = new DaoImpl(session, "hotelrating");        
            
            
            // and add some test records
            Random rnd = new Random();
            Instant instant = LocalDate.of(2014, 6, 29).atStartOfDay(ZoneId.of("UTC")).toInstant();
            
            for (int i = 0; i < 300; i++) {
                instant = instant.plus(Duration.ofSeconds(rnd.nextInt(59)));
                ratingsDao.writeEntity(new HotelRating(instant, 
                                                       String.valueOf(rnd.nextInt(50)), 
                                                       "user" + rnd.nextInt(100),
                                                       rnd.nextInt(5) + 1))
                          .execute();
            }
    
            instant = LocalDate.of(2014, 6, 30).atStartOfDay(ZoneId.of("UTC")).toInstant();
            for (int i = 0; i < 50; i++) {
                instant = instant.plus(Duration.ofSeconds(rnd.nextInt(59)));
                ratingsDao.writeEntity(new HotelRating(instant, 
                                                       String.valueOf(rnd.nextInt(50)), 
                                                       "user" + rnd.nextInt(100),
                                                       rnd.nextInt(5) + 1))
                          .execute();
            }
    
            
            
            // perform the example
            perform(ratingsDao);
        }
    }
    
    
    
    
    public static void perform(Dao ratingsDao) {
                
        // execute db query on time series table with day partition key (-> 2014-06-29, 2014-06-30, ...) 
        ImmutableList<Object> dayList = ImmutableList.of(LocalDate.of(2014, 6, 29).toEpochDay(),
                                                         LocalDate.of(2014, 6, 30).toEpochDay()); 
        Observable<HotelRating> ratingStream = RxReactiveStreams.toObservable(ratingsDao.readSequenceWithKeys("epoch_day", dayList)
                                                                                        .asEntity(HotelRating.class)
                                                                                        .executeRx());
        
        // create an aggregate (average rating) for each hotel in result
        Observable<AvgHotelRating> avgRatingStream = ratingStream.groupBy(rating -> rating.getHotelId())
                                                                 .flatMap(rating -> rating.reduce(new AvgHotelRating(rating.getKey(), 0, 0), (a, b) -> new AvgHotelRating(a.getHotelId(), 
                                                                                                                                                                          a.getRatingCount() + 1,
                                                                                                                                                                          a.getRatingSum() + b.getRating())));
        // consume stream
        RxReactiveStreams.subscribe(avgRatingStream, new ConsoleSubscriber());
        
        // prints out
        // > Hotel 44 was rated 3 on average by 8 visitors
        // > Hotel 45 was rated 3 on average by 7 visitors
        // > Hotel 46 was rated 3 on average by 7 visitors
        // > Hotel 47 was rated 3 on average by 5 visitors
        // > ...
    }    
}
