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

import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;

import net.oneandone.troilus.Field;


class HotelRating {

    @Field(name = "epoch_day")
    private long ratedAtEpochDay;
    
    @Field(name = "epoch_millis")
    private long ratedAtEpochMillis;

    @Field(name = "hotel_id")
    private String hotelId;

    @Field(name = "user_id")
    private String userId;
    
    @Field(name = "rating")
    private int rating;

    
    @SuppressWarnings("unused")
    private HotelRating() { }
    
    
    public HotelRating(Instant ratedAt, String hotelId, String userId, int rating) {
        this.ratedAtEpochDay = ratedAt.atZone(ZoneId.of("UTC")).toLocalDate().toEpochDay();
        this.ratedAtEpochMillis = ratedAt.toEpochMilli();
        this.hotelId = hotelId;
        this.userId = userId;
        this.rating = rating;
    }

    public LocalDate getRatedAtDay() {
        return LocalDate.ofEpochDay(ratedAtEpochDay);
    }

    public Instant getRatedAt() {
        return Instant.ofEpochMilli(ratedAtEpochMillis);
    }

    public String getHotelId() {
        return hotelId;
    }

    public String getUserId() {
        return userId;
    }

    public int getRating() {
        return rating;
    }
}
