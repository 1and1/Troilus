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

import java.io.IOException;

import net.oneandone.troilus.Cassandra;
import net.oneandone.troilus.Dao;
import net.oneandone.troilus.DaoImpl;
import net.oneandone.troilus.Record;

import org.reactivestreams.Publisher;
import org.reactivestreams.tck.PublisherVerification;
import org.reactivestreams.tck.TestEnvironment;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;



@Test
public class PublisherTest extends PublisherVerification<Record> {
    
    private static Cassandra cassandra;

    
    @BeforeTest
    public void setup() throws IOException {
        cassandra = Cassandra.create();
        
        cassandra.executeCql("CREATE TABLE publisher_test (table_id int  PRIMARY KEY)");
        for(int i = 0; i < maxElementsFromPublisher(); i++) {
            cassandra.executeCql("INSERT INTO publisher_test (table_id) VALUES (" + i + ")");
        }
    }

    @AfterTest
    public void teardown() {
        cassandra.close();
    }    
        
                
    
    public PublisherTest() {
        super(new TestEnvironment(2000), 250);
    }

    @Override
    public Publisher<Record> createPublisher(long elements) {
        try {
            Dao dao = new DaoImpl(cassandra.getSession(), "publisher_test");
            return dao.readAll().columns("table_id").withLimit((int)elements).execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    

    @Override
    public long maxElementsFromPublisher() {
        return 100l;
    }

    @Override
    public Publisher<Record> createErrorStatePublisher() {
        return null;
    }   
}