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

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.BiConsumer;

import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.NotFoundException;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;

import net.oneandone.reactive.pipe.Pipes;
import net.oneandone.reactive.rest.client.RxClient;
import net.oneandone.reactive.sse.ServerSentEvent;
import net.oneandone.reactive.sse.servlet.ServletSseSubscriber;
import net.oneandone.troilus.Dao;

import com.datastax.driver.core.ConsistencyLevel;


@Path("/")
public class HotelService implements Closeable {

    private final ScheduledExecutorService executor = Executors.newScheduledThreadPool(1);
    private final RxClient restClient = new RxClient(ClientBuilder.newClient());
    private final byte[] defaultPicture = new byte[] { 45, 56, 45, 45, 45};
    
    private final Dao hotelsDao;
    
 
    public HotelService(Dao hotelsDao) {
        this.hotelsDao = hotelsDao;
    }

    @Override
    public void close() throws IOException {
        executor.shutdown();
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
                 .whenComplete(ResultConsumer.writeTo(resp));
    }
    
    
    
    @Path("hotels/{id}/thumbnail")
    @GET
    @Produces("image/png")
    public void getHotelExternalViewAsync(@PathParam("id") String hotelId, 
                                          @PathParam("height") @DefaultValue("480") int height,  
                                          @PathParam("width") @DefaultValue("640") int width,
                                          @Suspended AsyncResponse resp) {


        hotelsDao.readWithKey("id", hotelId)
                 .asEntity(Hotel.class)
                 .withConsistency(ConsistencyLevel.QUORUM)
                 .executeAsync()
                 .thenApply(optionalHotel -> optionalHotel.<NotFoundException>orElseThrow(NotFoundException::new))
                 .thenCompose(hotel -> hotel.getPictureUri().map(uri -> restClient.target(uri)
                                                                                  .request()
                                                                                  .rx()
                                                                                  .get(byte[].class)
                                                                                  .exceptionally(error -> defaultPicture))
                                                            .orElse(CompletableFuture.completedFuture(defaultPicture)))
                 .thenCompose(picture -> Thumbnails.of(picture)
                                                   .size(height, width)
                                                   .outputFormat("image/png")
                                                   .computeAsync())    
                 .whenComplete(ResultConsumer.writeTo(resp));
        
        
         
    }
    
    /*

         
                  restClient.target(uri).request().rx().get(byte[].class)
                  .exceptionally(error -> defaultPicture
        .thenCompose(hotel -> restClient.target(hotel.getPictureUri())
                .request()
                .rx()
                .get(byte[].class)
                .exceptionally(error -> defaultPicture))
.thenCompose(picture -> Thumbnails.of(picture)
                   .size(height, width)
                   .outputFormat("image/png")
                   .computeAsync())                    


     */
    
    
    
    @Path("hotels")
    @GET
    @Produces("text/event-stream")
    public void getHotelsStreamAsync(@Context HttpServletResponse servletResponse, @Suspended AsyncResponse resp) throws IOException {
        // AsyncResponse param has to be set to "switch" into async mode implicitly
        
        servletResponse.setHeader("Content-Type", "text/event-stream");
        ServletOutputStream out = servletResponse.getOutputStream();
        
        hotelsDao.readAll()
                 .asEntity(Hotel.class)
                 .withConsistency(ConsistencyLevel.QUORUM)
                 .executeAsync()
                 .thenAccept(publisher -> Pipes.newPipe(publisher)
                                               .map(hotel -> ServerSentEvent.newEvent().data(hotel.getName()))
                                               .consume(new ServletSseSubscriber(out, executor)));
    }   
    
    
    


/**
 * ResultConsumer
 *
 */
private static  class ResultConsumer implements BiConsumer<Object, Throwable> {
    
    private final AsyncResponse asyncResponse;
    
    private ResultConsumer(AsyncResponse asyncResponse) {
        this.asyncResponse = asyncResponse;
    }

    
    @Override
    public void accept(Object result, Throwable error) {
        try {
            if (error == null) {
                asyncResponse.resume(result);
            } else {
                asyncResponse.resume(unwrapIfNecessary(error, 10));
            }
        } catch (RuntimeException rt) {
            System.out.println(rt);
        }
    }
    
    
    private static Throwable unwrapIfNecessary(Throwable ex, int maxDepth)  {

        if (isCompletionException(ex)) {
            Throwable e = ex.getCause();
            if (e != null) {
                if (maxDepth > 1) {
                    return unwrapIfNecessary(e, maxDepth - 1);
                } else {
                    return e;
                }
            }
        }
            
        return ex;
    }
    
    
    private static boolean isCompletionException(Throwable t) {
        return CompletionException.class.isAssignableFrom(t.getClass());
    }
    
    
    
    /**
     * forwards the response to the REST response object. Includes error handling also 
     * @param asyncResponse the REST response
     * @return the BiConsumer consuming the response/error pair
     */
    public static final BiConsumer<Object, Throwable> writeTo(AsyncResponse asyncResponse) {
        return new ResultConsumer(asyncResponse);
    }
}
}
