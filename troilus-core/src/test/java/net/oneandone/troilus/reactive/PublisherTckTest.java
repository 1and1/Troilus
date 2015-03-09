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
package net.oneandone.troilus.reactive;


import net.oneandone.troilus.Record;
import net.oneandone.troilus.SimpleResultList;

import org.reactivestreams.Publisher;
import org.reactivestreams.tck.PublisherVerification;
import org.reactivestreams.tck.TestEnvironment;
import org.testng.annotations.Test;



@Test
public class PublisherTckTest extends PublisherVerification<Record> {
    
    
    public PublisherTckTest() {
        super(new TestEnvironment(2000), 250);
    }

    @Override
    public long maxElementsFromPublisher() {
        return 100l;
    }

    @Override
    public Publisher<Record> createErrorStatePublisher() {
        return null;
    }
    
    @Override
    public Publisher<Record> createPublisher(long elements) {
        return SimpleResultList.newResultListPublisher(elements);
    }
}