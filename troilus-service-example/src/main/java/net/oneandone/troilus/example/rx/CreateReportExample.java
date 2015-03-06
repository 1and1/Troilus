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
import java.time.ZoneId;
import java.util.Random;

import net.oneandone.troilus.CassandraDB;
import net.oneandone.troilus.Dao;
import net.oneandone.troilus.DaoImpl;

import com.datastax.driver.core.Session;

import rx.Observable;
import rx.RxReactiveStreams;

 

public class CreateReportExample {
    
    
    public void printReport() {
        
    }

    public static void main(String[] args) throws InterruptedException, IOException {
        
        CassandraDB cassandra = CassandraDB.newInstance();
        cassandra.executeCql("CREATE TABLE hotelrating (rated_at_epoch_day bigInt, " + 
                             "                          rated_at_epoch_millis bigInt, " +
                             "                          hotel_id text, " + 
                             "                          user_id text, " + 
                             "                          rating int, " +
                             "                          PRIMARY KEY (rated_at_epoch_day, rated_at_epoch_millis));");
        Session session = cassandra.getSession();
        Dao ratingsDao = new DaoImpl(session, "hotelrating");        
        
        
        // add some test records
        Random rnd = new Random();
        for (int i = 0; i < 300; i++) {
            ratingsDao.writeEntity(new HotelRating(Instant.now(), 
                                                   String.valueOf(rnd.nextInt(50)), 
                                                   "user" + rnd.nextInt(100),
                                                   rnd.nextInt(5) + 1))
                      .execute();
        }

        
        perform(ratingsDao);
        
        session.close();
        cassandra.close();
    }
    
    
    
    public static void perform(Dao ratingsDao) {
                
        
        // execute db query
        long fromEpochDay = Instant.now().minus(Duration.ofHours(1)).atZone(ZoneId.of("UTC")).toLocalDate().toEpochDay();
        Observable<HotelRating> ratingStream = RxReactiveStreams.toObservable(ratingsDao.readSequenceWithKey("rated_at_epoch_day", fromEpochDay)
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
