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
package net.oneandone.troilus.example.service;

import java.util.Optional;
import java.util.UUID;

import javax.ws.rs.NotFoundException;
import javax.ws.rs.client.Client;








import net.oneandone.troilus.Dao;
import net.oneandone.troilus.DaoImpl;
import net.oneandone.troilus.example.service.Hotel;
import net.oneandone.troilus.example.service.HotelRepresentation;

import org.jboss.resteasy.client.jaxrs.ResteasyClientBuilder;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.datastax.driver.core.ConsistencyLevel;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.BaseEncoding;


//@Ignore
public class HotelServiceTest extends AbstractCassandraBasedTest {

    private static WebContainer server;

    @BeforeClass
    public static void beforeClass() throws Exception {
        
        server = new WebContainer("/service");
        server.setPort(8343);
        server.start();
        
        System.out.println(server.getBaseUrl());
        
        filldb();
    }

    @AfterClass
    public static void afterClass() throws Exception {
        server.stop();
    }
    
    
    @Test
    public void testGetHotel() throws Exception {
        Client client =  ResteasyClientBuilder.newClient();

        
        HotelRepresentation hotel = client.target(server.getBaseUrl() + "/hotels/BUP45544")
                                          .request()
                                          .get(HotelRepresentation.class);
        Assert.assertEquals("Corinthia Budapest", hotel.getName());
    }        


    
    @Test
    public void testPictureExample() throws Exception {
        Client client =  ResteasyClientBuilder.newClient();
        

        // hotel entry with valid URI
        byte[] picture = client.target(server.getBaseUrl() + "/hotels/BUP932432/thumbnail")
                               .request()
                               .get(byte[].class);
        
        Assert.assertEquals((byte) -46, picture[342]);   // hotel image thumbnail
        
        
        
        // hotel entry does not exits
        try {
            client.target(server.getBaseUrl() + "/hotels/BUPnotexits/thumbnail")
                  .request()
                  .get(byte[].class);
            Assert.fail("NotFoundException expected");
        } catch (NotFoundException expected) { } 


        // hotel entry with broken URI
        picture = client.target(server.getBaseUrl() + "/hotels/BUP14334/thumbnail")
                        .request()
                        .get(byte[].class);
        
        Assert.assertEquals((byte) 98, picture[342]);   // error image thumbnail
    }        

    
    @Test
    public void testPictureExampleObservable() throws Exception {
        Client client =  ResteasyClientBuilder.newClient();

        // hotel entry with valid URI
        byte[] picture = client.target(server.getBaseUrl() + "/observable/hotels/BUP932432/thumbnail")
                               .request()
                               .get(byte[].class);
        
        Assert.assertEquals((byte) -46, picture[342]);   // hotel image thumbnail
        
        
        
        // hotel entry does not exits
        try {
            client.target(server.getBaseUrl() + "/observable/hotels/BUPnotexits/thumbnail")
                  .request()
                  .get(byte[].class);
            Assert.fail("NotFoundException expected");
        } catch (NotFoundException expected) { } 

   
 /* DOES NOT WORK refer https://java.net/jira/browse/JERSEY-2813
        // hotel entry with broken URI
        byte[] picture = client.target(server.getBaseUrl() + "/observable/hotels/BUP14334/thumbnail")
                        .request()
                        .get(byte[].class);
        
        Assert.assertEquals((byte) 98, picture[342]);   // error image thumbnail
  */
    }        

    
    
    
    @Test
    public void testPictureExampleCallback() throws Exception {
        Client client =  ResteasyClientBuilder.newClient();
        
        
        byte[] picture = client.target(server.getBaseUrl() + "/hotels/BUP932432/thumbnail")
                .request()
                .get(byte[].class);

        Assert.assertEquals((byte) -46, picture[342]);   // hotel image thumbnail
        

        
        // hotel entry does not exits
        try {
            client.target(server.getBaseUrl() + "/classic/hotels/BUPnotexits/thumbnail")
                  .request()
                  .get(byte[].class);
            Assert.fail("NotFoundException expected");
        } catch (NotFoundException expected) { } 

        // hotel entry with broken URI
        picture = client.target(server.getBaseUrl() + "/classic/hotels/BUP14334/thumbnail")
                        .request()
                        .get(byte[].class);

        Assert.assertEquals((byte) 98, picture[342]);   // error image thumbnail
    }        

    
    
    
    
    private static void filldb() {
        Dao hotelsDao = new DaoImpl(getSession(), HotelsTable.TABLE);
        
        hotelsDao.writeEntity(new Hotel("BUP45544", 
                                        "Corinthia Budapest",
                                        ImmutableSet.of("1", "2", "3", "122", "123", "124", "322", "333"),
                                        null,
                                        Optional.of(5), 
                                        Optional.of("Superb hotel housed in a heritage building - exudes old world charm")
                                       ))
                 .execute();

  
        
        
        hotelsDao.writeWithKey("id", "BUP932432")
                 .value("name", "City Budapest")
                 .value("room_ids", ImmutableSet.of("1", "2", "3", "4", "5"))
                 .value("classification", 4)
                 .value("picture_uri", server.getBaseUrl()+ "/pictures/4545")
                 .execute();

   
    
        hotelsDao.writeEntity(new Hotel("BUP14334", 
                                        "Richter Panzio",
                                        ImmutableSet.of("1", "2", "3"),
                                        "http://localhost:" + server.getLocalPort() + "/doesnotexits",
                                        Optional.of(2), 
                                        Optional.empty())
                                        )
                  .withConsistency(ConsistencyLevel.ANY)
                  .execute();
    }    
}