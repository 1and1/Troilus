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
package net.oneandone.troilus.api;



import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import net.oneandone.troilus.Dao;
import net.oneandone.troilus.DaoImpl;
import net.oneandone.troilus.CassandraDB;
import net.oneandone.troilus.Record;
import net.oneandone.troilus.example.Address;
import net.oneandone.troilus.example.AddressType;
import net.oneandone.troilus.example.ClassifierEnum;
import net.oneandone.troilus.example.Hotel;
import net.oneandone.troilus.example.HotelsTable;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.datastax.driver.core.AbstractSession;
import com.datastax.driver.core.CloseFuture;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.RegularStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.ListenableFuture;
import com.sun.org.apache.bcel.internal.generic.NEWARRAY;



public class SessionReplacedTest {
    
    private static CassandraDB cassandra;
    
    
    
    @BeforeClass
    public static void beforeClass() throws IOException {
        cassandra = CassandraDB.newInstance();
    }
        
    @AfterClass
    public static void afterClass() throws IOException {
        cassandra.close();
    }


    
    @Before
    public void before() throws IOException {
        cassandra.tryExecuteCqlFile(AddressType.DDL);
        cassandra.tryExecuteCqlFile(HotelsTable.DDL);
    }
    
    
    @Test
    public void testExample() throws Exception {
                
        ReplacingSession session = new ReplacingSession(cassandra.newSession());
        
        
        // create dao
        Dao hotelsDao = new DaoImpl(session, HotelsTable.TABLE)
                                  .withConsistency(ConsistencyLevel.LOCAL_QUORUM)
                                  .withInterceptor(HotelsTable.CONSTRAINTS);
        
        
        ////////////////
        // inserts
        
        Hotel entity = new Hotel("BUP45544", 
                                 "Corinthia Budapest",
                                 ImmutableSet.of("1", "2", "3", "122", "123", "124", "322", "333"),
                                 Optional.of(ClassifierEnum.FIVE), 
                                 Optional.of("Superb hotel housed in a heritage building - exudes old world charm"),
                                 new Address("Erzsébet körút 43", "Budapest", "1073"),
                                 Optional.empty());
               
        hotelsDao.writeEntity(entity)
                 .ifNotExists()
                 .withConsistency(ConsistencyLevel.QUORUM)      
                 .withSerialConsistency(ConsistencyLevel.SERIAL)
                 .withWritetime(System.currentTimeMillis() * 10000)
                 .withTtl(Duration.ofDays(222))
                 .execute();
        
        Record record = hotelsDao.readWithKey(HotelsTable.ID, "BUP45544")
                                 .column(HotelsTable.NAME)
                                 .execute()
                                 .get();
        Assert.assertEquals("Corinthia Budapest", record.getString(HotelsTable.NAME));
        
        
        session.replaceSession(cassandra.newSession());
        
        try {
            record = hotelsDao.readWithKey(HotelsTable.ID, "BUP45544")
                    .column(HotelsTable.NAME)
                    .execute()
                    .get();
            Assert.assertEquals("Corinthia Budapest", record.getString(HotelsTable.NAME));
            Assert.fail("Exception exepected");
        } catch (Exception exepected) {  }
        

        int num = 25;
        CountDownLatch cdl = new CountDownLatch(num);
        AtomicBoolean isError = new AtomicBoolean(false);
        
        for (int i = 0; i < num; i++) {
            
            new Thread() {
                
                public void run() {
                    try {
                        Record record = hotelsDao.readWithKey(HotelsTable.ID, "BUP45544")
                                                 .column(HotelsTable.NAME)
                                                 .execute()
                                                 .get();
                        Assert.assertEquals("Corinthia Budapest", record.getString(HotelsTable.NAME));
                    } catch (Throwable t) {
                        isError.set(true);
                    }
                    cdl.countDown();
                };
            }.start();
            
        }

        cdl.await();
        

        Assert.assertFalse(isError.get());
    }
    
    
                   
    
    
    
    
    private class ReplacingSession extends AbstractSession {
        
        private final AtomicReference<Session> sessionRef = new AtomicReference<>();

        public ReplacingSession(Session session) {
            replaceSession(session);
        }
        
        public void replaceSession(Session session) {
            sessionRef.set(session);
        }
        
        public String getLoggedKeyspace() {
            return sessionRef.get().getLoggedKeyspace();
        }

        public Session init() {
            return sessionRef.get().init();
        }

        public ResultSet execute(String query) {
            return sessionRef.get().execute(query);
        }

        public ResultSet execute(String query, Object... values) {
            return sessionRef.get().execute(query, values);
        }

        public ResultSet execute(Statement statement) {
            return sessionRef.get().execute(statement);
        }

        public ResultSetFuture executeAsync(String query) {
            return sessionRef.get().executeAsync(query);
        }

        public ResultSetFuture executeAsync(String query, Object... values) {
            return sessionRef.get().executeAsync(query, values);
        }

        public ResultSetFuture executeAsync(Statement statement) {
            return sessionRef.get().executeAsync(statement);
        }

        public PreparedStatement prepare(String query) {
            return sessionRef.get().prepare(query);
        }

        public PreparedStatement prepare(RegularStatement statement) {
            return sessionRef.get().prepare(statement);
        }

        public ListenableFuture<PreparedStatement> prepareAsync(String query) {
            return sessionRef.get().prepareAsync(query);
        }

        public ListenableFuture<PreparedStatement> prepareAsync(
                RegularStatement statement) {
            return sessionRef.get().prepareAsync(statement);
        }

        public CloseFuture closeAsync() {
            return sessionRef.get().closeAsync();
        }

        public void close() {
            sessionRef.get().close();
        }

        public boolean isClosed() {
            return sessionRef.get().isClosed();
        }

        public Cluster getCluster() {
            return sessionRef.get().getCluster();
        }

        public State getState() {
            return sessionRef.get().getState();
        }
      /*  
        @Override
        protected ListenableFuture<PreparedStatement> prepareAsync(String query, Map<String, ByteBuffer> customPayload) {
            return null;
        }*/
    }
}


