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
package net.oneandone.troilus.example.utils.reactive.sse;





import java.io.FilterInputStream;

import java.io.IOException;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

import javax.servlet.ReadListener;
import javax.servlet.ServletInputStream;

import com.google.common.base.Joiner;
import com.google.common.base.MoreObjects;
import com.google.common.collect.Queues;



 
public class SSEReadableChannel {
    private final Queue<CompletableFuture<SSEEvent>> pendingReads = Queues.newLinkedBlockingQueue();

    private final SSEInputStream serverSentEventsStream;
    
    private final Consumer<Throwable> errorConsumer;
    private final Consumer<Void> completionConsumer;
    
    // statistics 
    private long numProcessedImmediate = 0;
    private long numProcessedPendings = 0;

    
    
    public SSEReadableChannel(ServletInputStream in, Consumer<Throwable> errorConsumer, Consumer<Void> completionConsumer) {
        this.errorConsumer = errorConsumer;
        this.completionConsumer = completionConsumer;
        this.serverSentEventsStream = new SSEInputStream(new NonBlockingInputStream(in));
        
        in.setReadListener(new ServletReadListener());
    }

    
    private final class ServletReadListener implements ReadListener {
        
        @Override
        public void onAllDataRead() throws IOException {
            completionConsumer.accept(null);
        }
        
        @Override
        public void onError(Throwable t) {
            SSEReadableChannel.this.onError(t);
        }
        
        @Override
        public void onDataAvailable() throws IOException {
            proccessPendingReads();
        }
    }

    
    public CompletableFuture<SSEEvent> readEventAsync() {
        CompletableFuture<SSEEvent> pendingRead = new CompletableFuture<SSEEvent>();
        
        synchronized (pendingReads) {   
            
            try {
                // reading has to be processed inside the sync block to avoid shuffling events 
                Optional<SSEEvent> optionalEvent = serverSentEventsStream.next();
                
                // got an event?
                if (optionalEvent.isPresent()) {
                    numProcessedImmediate++;
                    pendingRead.complete(optionalEvent.get());
                 
                // no, queue the pending read request    
                // will be handled by performing the read listener's onDataAvailable callback     
                } else {
                    pendingReads.add(pendingRead);
                }
                
            } catch (IOException | RuntimeException t) {
                onError(t);
            }
        }
        
        return pendingRead;
    }
    
    
    private void proccessPendingReads() {
        
        synchronized (pendingReads) {
            try {
                while(!pendingReads.isEmpty()) {
                    Optional<SSEEvent> optionalEvent = serverSentEventsStream.next();
                    if (optionalEvent.isPresent()) {
                        numProcessedPendings++;
                        pendingReads.poll().complete(optionalEvent.get());
                    } else {
                        return;
                    }
                }
            } catch (IOException | RuntimeException t) {
                onError(t);
            }
        }
    }
    
    
    private void onError(Throwable t)  {
        errorConsumer.accept(t);
        close();
    }
    
    
    public void close() {
        serverSentEventsStream.close();
        
        synchronized (pendingReads) { 
            pendingReads.clear();
        }
    }
    
    
    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                          .add("completionConsumer", completionConsumer)
                          .add("errorConsumer", errorConsumer)
                          .add("pendingReads", Joiner.on(", ").join(pendingReads))
                          .add("numProcessedTotal", numProcessedImmediate + numProcessedPendings)
                          .add("numProcessedImmediate", numProcessedImmediate)
                          .add("numProcessedPendings", numProcessedPendings)
                          .toString();
    }
    
    
    private static final class NonBlockingInputStream extends FilterInputStream  {
        private final ServletInputStream is;
        
        public NonBlockingInputStream(ServletInputStream is) {
            super(is);
            this.is = is;
        }
      
        
        @Override
        public int read(byte[] b) throws IOException {
            if (isNetworkdataAvailable()) {
                return super.read(b);
            } else {
                return 0;
            }
        }
        
        
        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            if (isNetworkdataAvailable()) {
                return super.read(b, off, len);
            } else {
                return 0;
            }
        }
        
        
        private boolean isNetworkdataAvailable() {
            try {
                return is.isReady();
            } catch (IllegalStateException ise)  {
                return false;
            }
        }
    }
}    
