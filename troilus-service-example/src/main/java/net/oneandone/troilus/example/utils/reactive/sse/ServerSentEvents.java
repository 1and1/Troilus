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








import java.util.concurrent.ScheduledExecutorService;


import javax.servlet.ServletInputStream;
import javax.servlet.ServletOutputStream;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;




public class ServerSentEvents {     

    private ServerSentEvents() { }
    
    public static Publisher<SSEEvent> newPublisher(ServletInputStream in) {
        return new SEEEventPublisher(in);
    }
    
    public static Subscriber<SSEEvent> newSubscriber(ServletOutputStream out, ScheduledExecutorService executor) {
        return new SEEEventSubscriber(out, executor);
    }
}



