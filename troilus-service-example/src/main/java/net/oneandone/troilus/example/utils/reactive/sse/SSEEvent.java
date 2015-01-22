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




/**
 * Event (Server-Sent Event. refer http://dev.w3.org/html5/eventsource/) 
 *
 * @author grro
 */
public interface SSEEvent {
    
    
    /**
     * @return the comment or <code>null</code>
     */
    String getComment();
   
    
    
    /**
     * @return the data ('data:' value)  or <code>null</code>
     */
    String getData();
    
    
    /**
     * @return the event type ('event:' value) or <code>null</code>
     */
    String getEvent();
  
    
    /**
     * @return the id ('id:' value) or <code>null</code>
     */
    String getId();

    
    /**
     * @return the retry ('retry:' value) or <code>null</code>
     */
    Integer getRetry();
    


    /**
     * @return the "wire" formated event according to Server-Sent Events spec
     */
    @Override
    String toString();
  
    
    default String toWire() {
        StringBuilder sb = new StringBuilder();
        
        if (getComment() != null) {
            sb.append(": " + getComment() + "\r\n");
        }

        if (getId() != null) {
            sb.append("id: " + getId() + "\r\n");
        }
        
        if (getEvent() != null) {
            sb.append("event: " + getEvent() + "\r\n");
        }

        if (getData() != null) {
            sb.append("data: " + getData() + "\r\n");
        }
        
        if (getRetry() != null) {
            sb.append("retry: " + getRetry() + "\r\n");
        }
        
        sb.append("\r\n");
        
        return sb.toString();
    }
    
    
    
    public static SSEBuildableEvent newEvent() {
        return new SSEBuildableEvent(null, null, null, null, null);
    }
    
    
    
    
    public static class SSEBuildableEvent implements SSEEvent {
        private final String id;
        private final String event;
        private final String data;
        private final String comment;
        private final Integer retry;


        SSEBuildableEvent(String id, String event, String data, String comment, Integer retry) {
            this.id = id;
            this.event = event;
            this.data = data;
            this.comment = comment;
            this.retry = retry;
        }
        
        
        public SSEBuildableEvent data(String data) {
            return new SSEBuildableEvent(this.id,
                                         this.event, 
                                         data, 
                                         this.comment, 
                                         this.retry); 
        }
        
        public SSEBuildableEvent id(String id) {
            return new SSEBuildableEvent(id, 
                                         this.event, 
                                         this.data, 
                                         this.comment, 
                                         this.retry); 
        }
        
        public SSEBuildableEvent comment(String comment) {
            return new SSEBuildableEvent(this.id, 
                                         this.event, 
                                         this.data, 
                                         comment, 
                                         this.retry); 
        }
        
        public SSEBuildableEvent event(String event) {
            return new SSEBuildableEvent(this.id, 
                                         event, 
                                         this.data, 
                                         this.comment,
                                         this.retry); 
        }
        
        public SSEBuildableEvent retry(Integer retry) {
            return new SSEBuildableEvent(this.id, 
                                         this.event, 
                                         this.data, 
                                         this.comment,  
                                         retry); 
        }

        @Override
        public String getComment() {
            return comment;
        }
        
        @Override
        public String getData() {
            return data;
        }

        @Override
        public String getEvent() {
            return event;
        }

        @Override
        public String getId() {
            return id;
        }


        public Integer getRetry() {
            return retry;
        }
        
        boolean isUserEvent() {
            return (getData() != null) || (getEvent() != null);
        }
        
        
        @Override
        public String toString() {
            return toWire();
        }    
        
        
        @Override
        public boolean equals(Object other) {
            return (other instanceof SSEEvent) ? ((SSEEvent) other).toWire().equals(this.toWire()) : false;
        }
        
        @Override
        public int hashCode() {
           return toWire().hashCode();
        }
    }
}