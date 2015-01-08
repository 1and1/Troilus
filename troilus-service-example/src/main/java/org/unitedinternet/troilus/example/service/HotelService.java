/*
 * Copyright (c) 2015 1&1 Internet AG, Germany, http://www.1und1.de
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *  http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.unitedinternet.troilus.example.service;

import javax.ws.rs.GET;
import javax.ws.rs.NotFoundException;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.MediaType;

import org.unitedinternet.troilus.example.utils.ResultConsumer;

import com.datastax.driver.core.ConsistencyLevel;
import com.unitedinternet.troilus.Dao;


@Path("/")
public class HotelService {

    private final Dao hotelsDao;
 
    public HotelService() {
        this(null);
    }
    
    public HotelService(Dao hotelsDao) {
        this.hotelsDao = hotelsDao;
    }
    
    @Path("hotels/{id}")
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public void getHotelsAsync(@PathParam("id") String hotelId, @Suspended AsyncResponse resp) {
        
        hotelsDao.readWithKey("id", hotelId)
                 .asEntity(Hotel.class)
                 .withConsistency(ConsistencyLevel.QUORUM)
                 .executeAsync()
                 .thenApply(optionalHotel -> optionalHotel.<NotFoundException>orElseThrow(NotFoundException::new))
                 .thenApply(hotel -> new HotelRepresentation(hotel.getId(), hotel.getName(), hotel.getRoomIds()))
                 .whenComplete(ResultConsumer.write(resp));
    }
}
