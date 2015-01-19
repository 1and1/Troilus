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
package net.oneandone.troilus.example.utils.reactive.sse;






import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import javax.servlet.ServletOutputStream;
import javax.servlet.WriteListener;

import com.google.common.collect.Lists;


public class SSEWriteableChannel {
    private static final List<CompletableFuture<SSEWriteableChannel>> whenWritePossibles = Lists.newArrayList();
    
    private final ServletOutputStream out;
    private final SSEOutputStream serverSentEventsStream;
    private final Consumer<Throwable> errorConsumer;

    
    public SSEWriteableChannel(ServletOutputStream out, Consumer<Throwable> errorConsumer) {
        this(out, errorConsumer, null);
    }


    
    public SSEWriteableChannel(ServletOutputStream out, Consumer<Throwable> errorConsumer, ScheduledExecutorService executor) {
        this.out = out;
        this.serverSentEventsStream = new SSEOutputStream(out);
        this.errorConsumer = errorConsumer;
        
        out.setWriteListener(new ServletWriteListener());
        
        if (executor != null) {
            // start the keep alive emitter 
            new KeepAliveEmitter(this, executor).start();
        }
        
        whenWritePossibleAsync().thenAccept(channel -> channel.flush());
    }

    
    public CompletableFuture<Integer> writeEventAsync(SSEEvent event) {       
        CompletableFuture<Integer> result = new CompletableFuture<>();
        
        try {
            synchronized (serverSentEventsStream) {
                byte[] data = event.toWire().getBytes("UTF-8");
                serverSentEventsStream.write(event);
                
                result.complete(data.length);
            }
            
        } catch (IOException | RuntimeException t) {
            errorConsumer.accept(t);
            result.completeExceptionally(t);
        }
        
        return result;
    }   

    
    private void flush() {
        try {
            serverSentEventsStream.flush();
        } catch (IOException ioe) {
            throw new RuntimeException(ioe);
        }
    }
    
    
    public void close() {
        try {
            serverSentEventsStream.close();
        } catch (IOException ignore) { }
    }
    
    
    public CompletableFuture<SSEWriteableChannel> whenWritePossibleAsync() {
        CompletableFuture<SSEWriteableChannel> whenWritePossible = new CompletableFuture<SSEWriteableChannel>();

        synchronized (whenWritePossibles) {
            if (isWritePossible()) {
                whenWritePossible.complete(SSEWriteableChannel.this);
            } else {
                // if not the WriteListener#onWritePossible will be called by the servlet conatiner later
                whenWritePossibles.add(whenWritePossible);
            }
        }
        
        return whenWritePossible;
    }
    

    
    private boolean isWritePossible() {
        
        // triggers that write listener's onWritePossible will be called, if is possible to write data
        // According to the Servlet 3.1 spec the onWritePossible will be invoked if and only if isReady() 
        // method has been called and has returned false.
        //
        // Unfortunately the Servlet 3.1 spec left it open how many bytes can be written
        // Jetty for instance keeps a reference to the passed byte array and essentially owns it until the write is complete
        
        try {
            return out.isReady();
        } catch (IllegalStateException ise)  {
            return false;
        }
    }
    
    
    private final class ServletWriteListener implements WriteListener {

        @Override
        public void onWritePossible() throws IOException {    
            
            synchronized (whenWritePossibles) {
                whenWritePossibles.forEach(whenWritePossible -> whenWritePossible.complete(SSEWriteableChannel.this));
                whenWritePossibles.clear();
            }
        }

        @Override
        public void onError(Throwable t) {
            errorConsumer.accept(t);
        }
    }  
    
    
    
    /**
     * sents keep alive messages to keep the http connection alive in case of idling
     * @author grro
     */
    private static final class KeepAliveEmitter {
        private final Duration noopPeriodSec = Duration.ofSeconds(35); 
        
        private final SSEWriteableChannel channel;
        private final ScheduledExecutorService executor;

        
        public KeepAliveEmitter(SSEWriteableChannel channel, ScheduledExecutorService executor) {
            this.channel = channel;
            this.executor = executor;
        }
        
        public void start() {
            scheduleNextKeepAliveEvent();
        }
        
        private void scheduleNextKeepAliveEvent() {
            Runnable task = () -> channel.whenWritePossibleAsync()
                                         .thenAccept(Void -> channel.writeEventAsync(SSEEvent.newEvent().comment("keep alive"))
                                                                    .thenAccept(written -> scheduleNextKeepAliveEvent()));

            executor.schedule(task, noopPeriodSec.getSeconds(), TimeUnit.SECONDS);
        }
        
    }
}
