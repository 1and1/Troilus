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
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Date;
import java.util.Random;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import net.oneandone.troilus.CassandraDB;
import net.oneandone.troilus.Dao;
import net.oneandone.troilus.DaoImpl;

import org.reactivestreams.Publisher;

import com.datastax.driver.core.Session;

import rx.Observable;
import rx.RxReactiveStreams;
import rx.observables.GroupedObservable;



public class HotelExample {

    
    public static void main(String[] args) throws InterruptedException, IOException {
        
        // bootstrap embedded Cassandra instance
        CassandraDB cassandra = CassandraDB.newInstance();
        cassandra.executeCql("CREATE TABLE hotelrating (rated_at timestamp primary key, hotel_id text, user_id text, rating int);");
        Session session = cassandra.getSession();
        Dao ratingsDao = new DaoImpl(session, "hotelrating");
        
        // get all hotel ratings during last hour from Cassandra
        Date from = Date.from(LocalDateTime.now().minusHours(1).atZone(ZoneOffset.systemDefault()).toInstant());
        Observable<HotelRating> ratingsLastHour = recentRatings(ratingsDao, from);

        // create an aggregate (average rating) for each hotel in result
        Observable<AvgHotelRating> aggregates = HotelExample.createAggregates(ratingsLastHour);

        // For this example we'll simply write all results to stdout. Its just an observable at this point,
        // so we could as well consume the ratings in a totally different way e.g. by writing back to Cassandra.
        System.out.println("--------------Recent average ratings--------------");
        aggregates.forEach(h -> 
            System.out.println(String.format("Hotel %s was rated %d on average by %d visitors", 
                h.getHotelId(), h.getRatingSum() / h.getRatingCount(), h.getRatingCount()))
        );
        
        session.close();
        cassandra.close();
    }

    // returns recent ratings from cassandra
    private static Observable<HotelRating> recentRatings(Dao ratingsDao, Date from) {
        
        Publisher<HotelRating> resultPublisher = ratingsDao.readSequenceWithKey("rated_at", from)
                .asEntity(HotelRating.class)
                .executeRx();
        
        // convert Publisher to Observable
        Observable<HotelRating> observable = RxReactiveStreams.toObservable(resultPublisher); 
        
        // merge empty result with some random data for demo purposes
        return Observable.merge(observable, randomRatings());
    }

    // please note how such stream transformations can be put into a seperate class method 
    // and re-used accross your application - this is what makes streams composable
    private static Observable<AvgHotelRating> createAggregates(Observable<HotelRating> hotelObsv) {

        // get observable for each hotel id along with another observable with all hotel ratings for the hotel
        Observable<GroupedObservable<String, HotelRating>> gb = hotelObsv.groupBy(h -> h.getHotelId());

        // create aggregates on hotel ratings
        Observable<AvgHotelRating> avgRatings = gb.flatMap(h -> h.reduce(new AvgHotelRating(h.getKey(), 0, 0), (a, b) ->
            // reduce all ratings for each hotel into a single AvgHotelRating by counting the number
            // of ratings and summing up all rating values so we can build an average later
            new AvgHotelRating(a.getHotelId(), a.getRatingCount() + 1, a.getRatingSum() + b.getRating())));

        return avgRatings;
    }
    
    // generates some random dummy ratings
    private static Observable<HotelRating> randomRatings() {
        Random rnd = new Random();
        Stream<HotelRating> s = IntStream.range(0, 1000).mapToObj(
                i -> new HotelRating(0L, String.valueOf(rnd.nextInt(50)), "user" + rnd.nextInt(100), rnd.nextInt(5) + 1));
        return Observable.from(() -> s.iterator());
    }

}
