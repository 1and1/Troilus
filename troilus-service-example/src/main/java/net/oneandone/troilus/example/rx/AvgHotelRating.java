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


class AvgHotelRating {

    private String hotelId;
    private Integer ratingCount;
    private Integer ratingSum;

    public AvgHotelRating(String hotelId, Integer ratingCount, Integer ratingSum) {
        super();
        this.hotelId = hotelId;
        this.ratingCount = ratingCount;
        this.ratingSum = ratingSum;
    }

    public String getHotelId() {
        return hotelId;
    }

    public Integer getRatingCount() {
        return ratingCount;
    }

    public Integer getRatingSum() {
        return ratingSum;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((hotelId == null) ? 0 : hotelId.hashCode());
        result = prime * result + ((ratingCount == null) ? 0 : ratingCount.hashCode());
        result = prime * result + ((ratingSum == null) ? 0 : ratingSum.hashCode());
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
        AvgHotelRating other = (AvgHotelRating) obj;
        if (hotelId == null) {
            if (other.hotelId != null)
                return false;
        } else if (!hotelId.equals(other.hotelId))
            return false;
        if (ratingCount == null) {
            if (other.ratingCount != null)
                return false;
        } else if (!ratingCount.equals(other.ratingCount))
            return false;
        if (ratingSum == null) {
            if (other.ratingSum != null)
                return false;
        } else if (!ratingSum.equals(other.ratingSum))
            return false;
        return true;
    }

}