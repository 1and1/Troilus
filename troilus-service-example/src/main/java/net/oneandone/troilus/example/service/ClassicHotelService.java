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

import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;

import javax.imageio.ImageIO;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.NotFoundException;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.InvocationCallback;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;

import org.imgscalr.Scalr;
import org.imgscalr.Scalr.Mode;

import net.oneandone.reactive.rest.client.RxClient;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.google.common.io.Resources;


@Path("/classic/hotels")
public class ClassicHotelService {

    private final RxClient restClient = new RxClient(ClientBuilder.newClient());
    private final byte[] defaultPicture;
    
    private final Session session;
    private final PreparedStatement preparedSelectStmt;
 
    
    public ClassicHotelService(Session session) throws IOException {
        this.session = session;
        defaultPicture = Resources.toByteArray(Resources.getResource("error.jpg"));
        preparedSelectStmt = session.prepare("select id, name, description, classification, picture_uri, room_ids from hotels where id = ?");
        preparedSelectStmt.setConsistencyLevel(ConsistencyLevel.QUORUM);
    }
    

    @Path("/{id}/thumbnail")
    @GET
    @Produces("image/png")
    public void getHotelThumbnailAsync(@PathParam("id") String hotelId, 
                                       @PathParam("height") @DefaultValue("80") int height,  
                                       @PathParam("width") @DefaultValue("80") int width,
                                       @Suspended AsyncResponse resp) {
        
        // (1) call the database
        ResultSetFuture databaseResponseFuture = session.executeAsync(preparedSelectStmt.bind(hotelId));
        databaseResponseFuture.addListener(new Runnable() {
            
            public void run() {
                try {
                    Row row = databaseResponseFuture.get().one();
                    if (row == null) {
                        resp.resume(new NotFoundException());
                        
                    } else {
                        
                        // (2) call the rest service to get the hotel picture
                        restClient.target(row.getString("picture_uri"))
                                  .request()
                                  .async()
                                  .get(new InvocationCallback<byte[]>() {
    
                                      public void failed(Throwable throwable) {
                                          completed(defaultPicture);
                                      }
                                      
                                      public void completed(byte[] picture) {
    
                                          // (3) call the thumbnail library to reduce the  size
                                          try {
                                              byte[] thumbnail = resize(picture, height, width, "png"); 
                                              resp.resume(thumbnail);
                                          } catch (RuntimeException e) {
                                              resp.resume(e);
                                          }                              
                                      }
                                });
    
                    }
                } catch (ExecutionException | InterruptedException | RuntimeException e) {
                    resp.resume(e);
                }
            }
        }, ForkJoinPool.commonPool());
    }
    
    
    private byte[] resize(byte[] picture, int height, int width, String format) {
        try {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            
            BufferedImage img = ImageIO.read(new ByteArrayInputStream(picture));
            BufferedImage scaledImg = Scalr.resize(img, Mode.AUTOMATIC, height, width);
               
            ImageIO.write(scaledImg, format, bos);
            return bos.toByteArray();
        } catch (IOException | RuntimeException e) {
            throw new RuntimeException(e);
        }
    }
   
}