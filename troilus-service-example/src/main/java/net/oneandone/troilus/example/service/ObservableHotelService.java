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

import javax.imageio.ImageIO;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;

import org.glassfish.jersey.client.rx.Rx;
import org.glassfish.jersey.client.rx.RxClient;
import org.glassfish.jersey.client.rx.rxjava.RxObservableInvoker;
import org.imgscalr.Scalr;
import org.imgscalr.Scalr.Mode;

import rx.RxReactiveStreams;
import net.oneandone.reactive.rest.container.ObservableConsumer;
import net.oneandone.troilus.Dao;

import com.google.common.io.Resources;


@Path("observable/hotels")
public class ObservableHotelService {

    private final RxClient<RxObservableInvoker> restClient = Rx.newClient(RxObservableInvoker.class);
    private final byte[] defaultPicture;
    
    private final Dao hotelsDao;
    
 
    public ObservableHotelService(Dao hotelsDao) throws IOException {
        this.hotelsDao = hotelsDao;
        defaultPicture = Resources.toByteArray(Resources.getResource("hotel.png"));
    }

    
    
    @Path("/{id}/thumbnail")
    @GET
    @Produces("image/png")
    public void getHotelThumbnailObservableAsync(@PathParam("id") String hotelId, 
                                       @PathParam("height") @DefaultValue("160") int height,  
                                       @PathParam("width") @DefaultValue("160") int width,
                                       @Suspended AsyncResponse resp) {

        
        hotelsDao.readSequenceWithKey("id", hotelId)  
                 .asEntity(Hotel.class)
                 .executeAsync()                                                                 
                 .thenApply(publisher -> RxReactiveStreams.toObservable(publisher))
                 .thenApply(observable -> observable.flatMap(hotel -> restClient.target(hotel.getPictureUri())                  
                                                                                .request()                 
                                                                                .rx()
                                                                                .get(byte[].class)
                                                                                .onErrorReturn(error -> defaultPicture))
                                                    .map(picture -> resize(picture, height, width, "png")))
                 .whenComplete(ObservableConsumer.writeSingleTo(resp));  
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
