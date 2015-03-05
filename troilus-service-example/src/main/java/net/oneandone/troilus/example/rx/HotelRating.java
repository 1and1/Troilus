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

import net.oneandone.troilus.Field;


class HotelRating {

    @Field(name = "rated_at")
    private Long ratedAt = null;

    @Field(name = "hotel_id")
    private String hotelId = null;

    @Field(name = "user_id")
    private String userId = null;
    
    @Field(name = "rating")
    private Integer rating;

    
    public HotelRating(Long ratedAt, String hotelId, String userId, Integer rating) {
        super();
        this.ratedAt = ratedAt;
        this.hotelId = hotelId;
        this.userId = userId;
        this.rating = rating;
    }

    public Long getRatedAt() {
        return ratedAt;
    }

    public String getHotelId() {
        return hotelId;
    }

    public String getUserId() {
        return userId;
    }

    public Integer getRating() {
        return rating;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((hotelId == null) ? 0 : hotelId.hashCode());
        result = prime * result + ((ratedAt == null) ? 0 : ratedAt.hashCode());
        result = prime * result + ((rating == null) ? 0 : rating.hashCode());
        result = prime * result + ((userId == null) ? 0 : userId.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        HotelRating other = (HotelRating) obj;
        if (hotelId == null) {
            if (other.hotelId != null)
                return false;
        } else if (!hotelId.equals(other.hotelId))
            return false;
        if (ratedAt == null) {
            if (other.ratedAt != null)
                return false;
        } else if (!ratedAt.equals(other.ratedAt))
            return false;
        if (rating == null) {
            if (other.rating != null)
                return false;
        } else if (!rating.equals(other.rating))
            return false;
        if (userId == null) {
            if (other.userId != null)
                return false;
        } else if (!userId.equals(other.userId))
            return false;
        return true;
    }

    
    
}
