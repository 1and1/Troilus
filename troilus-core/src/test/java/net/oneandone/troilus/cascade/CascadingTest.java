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
package net.oneandone.troilus.cascade;



import java.io.IOException;
import java.util.Optional;




import java.util.concurrent.CompletableFuture;

import net.oneandone.troilus.Batchable;
import net.oneandone.troilus.CassandraDB;
import net.oneandone.troilus.Dao;
import net.oneandone.troilus.DaoImpl;
import net.oneandone.troilus.DeleteQueryData;
import net.oneandone.troilus.Record;
import net.oneandone.troilus.interceptor.CascadeOnDeleteInterceptor;
import net.oneandone.troilus.interceptor.CascadeOnWriteInterceptor;
import net.oneandone.troilus.interceptor.WriteQueryData;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import com.datastax.driver.core.CodecRegistry;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.TupleType;
import com.datastax.driver.core.exceptions.InvalidQueryException;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.google.common.collect.ImmutableSet;


@Ignore
public class CascadingTest {

    private static CassandraDB cassandra;
    
    // 3.x API change
    private ProtocolVersion protocolVersion = ProtocolVersion.NEWEST_SUPPORTED;
    // 3.x API change
    private CodecRegistry codecRegistry = CodecRegistry.DEFAULT_INSTANCE;
    
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
        cassandra.tryExecuteCqlFile(KeyByAccountColumns.DDL);
        cassandra.tryExecuteCqlFile(KeyByEmailColumns.DDL);
    }
    

    @Test
    public void testCasscading() throws Exception {   
        
        DaoManager daoManager = new DaoManager(cassandra.getSession());
       
        Dao keyByAccountDao = daoManager.getKeyByAccountDao();
        Dao keyByEmailDao = daoManager.getKeyByEmailDao();

        
        String id = "act3344";
        byte[] key = new byte[] { 34, 56, 87, 88 };
        String email = "me@example.org";
        long time = System.currentTimeMillis(); 
        
        
        //////////////////////////////////////
        // insert 
       // TupleType idxType = TupleType.of(DataType.text(), DataType.bigint());
        TupleType idxType = TupleType.of(protocolVersion, codecRegistry,DataType.text(), DataType.bigint());
        keyByAccountDao.writeWithKey(KeyByAccountColumns.ACCOUNT_ID, id)
                       .value(KeyByAccountColumns.KEY, key)
                       .addSetValue(KeyByAccountColumns.EMAIL_IDX, idxType.newValue(email, time))
                       .withConsistency(ConsistencyLevel.QUORUM)
                       .execute();
        
        
        
        // test 
        Record record = keyByEmailDao.readWithKey(KeyByEmailColumns.EMAIL, email, KeyByEmailColumns.CREATED, time)
                                     .withConsistency(ConsistencyLevel.QUORUM)
                                     .execute()
                                     .get();
        Assert.assertEquals(id, record.getValue(KeyByEmailColumns.ACCOUNT_ID));
        Assert.assertArrayEquals(key, record.getValue(KeyByEmailColumns.KEY));
        
        record = keyByAccountDao.readWithKey(KeyByAccountColumns.ACCOUNT_ID, id)
                                .withConsistency(ConsistencyLevel.QUORUM)
                                .execute()
                                .get();
        Assert.assertEquals(id, record.getValue(KeyByAccountColumns.ACCOUNT_ID));
        Assert.assertEquals(email, record.getValue(KeyByAccountColumns.EMAIL_IDX).iterator().next().getString(0));
        Assert.assertEquals(time, record.getValue(KeyByAccountColumns.EMAIL_IDX).iterator().next().getLong(1));
        
        
        
        
        
        
        
        ///////////////////////////////////////////////////////
        // Delete
        
        keyByAccountDao.deleteWithKey(KeyByAccountColumns.ACCOUNT_ID, id)
                       .withConsistency(ConsistencyLevel.QUORUM)
                       .execute();
        
        
        
        Assert.assertEquals(Optional.empty(), keyByEmailDao.readWithKey(KeyByEmailColumns.EMAIL, email, KeyByEmailColumns.CREATED, time)
                                                           .withConsistency(ConsistencyLevel.QUORUM)
                                                           .execute());

        Assert.assertEquals(Optional.empty(), keyByAccountDao.readWithKey(KeyByAccountColumns.ACCOUNT_ID, id)
                                                             .withConsistency(ConsistencyLevel.QUORUM)
                                                             .execute());        
    }
    

    

    @Test
    public void testCasscading2() throws Exception {   
        
        DaoManager daoManager = new DaoManager(cassandra.getSession());
       
        Dao keyByAccountDao = daoManager.getKeyByAccountDao();
        Dao keyByEmailDao = daoManager.getKeyByEmailDao();

        
        String id = "act3344";
        byte[] key = new byte[] { 34, 56, 87, 88 };
        String email = "me@example.org";
        long time = System.currentTimeMillis(); 
        
        
        //////////////////////////////////////
        // insert 
        // 3.x API change
        //TupleType idxType = TupleType.of(DataType.text(), DataType.bigint());
        TupleType idxType = TupleType.of(protocolVersion, codecRegistry,DataType.text(), DataType.bigint());
        keyByAccountDao.writeWithKey(KeyByAccountColumns.ACCOUNT_ID, id)
                       .value(KeyByAccountColumns.KEY, key)
                       .value(KeyByAccountColumns.EMAIL_IDX, ImmutableSet.of(idxType.newValue(email, time)))
                       .withConsistency(ConsistencyLevel.QUORUM)
                       .execute();
        
        
        
        // test 
        Record record = keyByEmailDao.readWithKey(KeyByEmailColumns.EMAIL, email, KeyByEmailColumns.CREATED, time)
                                     .withConsistency(ConsistencyLevel.QUORUM)
                                     .execute()
                                     .get();
        Assert.assertEquals(id, record.getValue(KeyByEmailColumns.ACCOUNT_ID));
        Assert.assertArrayEquals(key, record.getValue(KeyByEmailColumns.KEY));
        
        record = keyByAccountDao.readWithKey(KeyByAccountColumns.ACCOUNT_ID, id)
                                .withConsistency(ConsistencyLevel.QUORUM)
                                .execute()
                                .get();
        Assert.assertEquals(id, record.getValue(KeyByAccountColumns.ACCOUNT_ID));
        Assert.assertEquals(email, record.getValue(KeyByAccountColumns.EMAIL_IDX).iterator().next().getString(0));
        Assert.assertEquals(time, record.getValue(KeyByAccountColumns.EMAIL_IDX).iterator().next().getLong(1));
        
        
        
        
        
        
        
        ///////////////////////////////////////////////////////
        // Delete
        
        keyByAccountDao.deleteWithKey(KeyByAccountColumns.ACCOUNT_ID, id)
                       .withConsistency(ConsistencyLevel.QUORUM)
                       .execute();
        
        
        
        Assert.assertEquals(Optional.empty(), keyByEmailDao.readWithKey(KeyByEmailColumns.EMAIL, email, KeyByEmailColumns.CREATED, time)
                                                           .withConsistency(ConsistencyLevel.QUORUM)
                                                           .execute());

        Assert.assertEquals(Optional.empty(), keyByAccountDao.readWithKey(KeyByAccountColumns.ACCOUNT_ID, id)
                                                             .withConsistency(ConsistencyLevel.QUORUM)
                                                             .execute());        
    }
    
    
    

    @Test
    public void testCasscadingNoIndex() throws Exception {   
        
        DaoManager daoManager = new DaoManager(cassandra.getSession());
       
        Dao keyByAccountDao = daoManager.getKeyByAccountDao();
        Dao keyByEmailDao = daoManager.getKeyByEmailDao();

        
        String id = "act3344";
        byte[] key = new byte[] { 34, 56, 87, 88 };
        String email = "me@example.org";
        long time = System.currentTimeMillis(); 
        
        
        //////////////////////////////////////
        // insert 
        keyByAccountDao.writeWithKey(KeyByAccountColumns.ACCOUNT_ID, id)
                       .value(KeyByAccountColumns.KEY, key)
                       .withConsistency(ConsistencyLevel.QUORUM)
                       .execute();
        
        
        
        // test 
        Optional<Record> optionalRecord = keyByEmailDao.readWithKey(KeyByEmailColumns.EMAIL, email, KeyByEmailColumns.CREATED, time)
                                                       .withConsistency(ConsistencyLevel.QUORUM)
                                                       .execute();
        Assert.assertFalse(optionalRecord.isPresent());
        
        Record record = keyByAccountDao.readWithKey(KeyByAccountColumns.ACCOUNT_ID, id)
                                       .withConsistency(ConsistencyLevel.QUORUM)
                                       .execute()
                                       .get();
        Assert.assertEquals(id, record.getValue(KeyByAccountColumns.ACCOUNT_ID));
        Assert.assertTrue(record.getValue(KeyByAccountColumns.EMAIL_IDX).isEmpty());
        
        
        
        
        
        ///////////////////////////////////////////////////////
        // Delete
        
        keyByAccountDao.deleteWithKey(KeyByAccountColumns.ACCOUNT_ID, id)
                       .withConsistency(ConsistencyLevel.QUORUM)
                       .execute();
        
        Assert.assertEquals(Optional.empty(), keyByAccountDao.readWithKey(KeyByAccountColumns.ACCOUNT_ID, id)
                                                             .withConsistency(ConsistencyLevel.QUORUM)
                                                             .execute());        
    }
    
    
    

    @Test
    public void testCasscadingUnssuportedInsert() throws Exception {   
        
        DaoManager daoManager = new DaoManager(cassandra.getSession());
       
        Dao keyByAccountDao = daoManager.getKeyByAccountDao();

        
        String id = "act334345345344";
        byte[] key = new byte[] { 34, 56, 87, 88 };
        String email = "me@example.org";
        long time = System.currentTimeMillis(); 
        
        
        //////////////////////////////////////
        // insert
        // 3.x API change
        //TupleType idxType = TupleType.of(DataType.text(), DataType.bigint());
        TupleType idxType = TupleType.of(protocolVersion, codecRegistry,DataType.text(), DataType.bigint());
        try {
            keyByAccountDao.writeWhere(QueryBuilder.in(KeyByAccountColumns.ACCOUNT_ID.getName(), id))
                           .value(KeyByAccountColumns.KEY, key)
                           .addSetValue(KeyByAccountColumns.EMAIL_IDX, idxType.newValue(email, time))
                           .withConsistency(ConsistencyLevel.QUORUM)
                           .execute();
            Assert.fail("InvalidQueryException expected");
        } catch (InvalidQueryException expected) {}        
    
    }

    
    

    @Test
    public void testCasscadingLwtDelete() throws Exception {   
        
        DaoManager daoManager = new DaoManager(cassandra.getSession());
       
        Dao keyByAccountDao = daoManager.getKeyByAccountDao();
        Dao keyByEmailDao = daoManager.getKeyByEmailDao();

        
        String id = "act3344";
        byte[] key = new byte[] { 34, 56, 87, 88 };
        String email = "me@example.org";
        long time = System.currentTimeMillis(); 
        
        
        //////////////////////////////////////
        // insert 
        // 3.x API change
        //TupleType idxType = TupleType.of(DataType.text(), DataType.bigint());    
        TupleType idxType = TupleType.of(protocolVersion, codecRegistry,DataType.text(), DataType.bigint());
        keyByAccountDao.writeWithKey(KeyByAccountColumns.ACCOUNT_ID, id)
                       .value(KeyByAccountColumns.KEY, key)
                       .addSetValue(KeyByAccountColumns.EMAIL_IDX, idxType.newValue(email, time))
                       .withConsistency(ConsistencyLevel.QUORUM)
                       .execute();
        
        
        
        // test 
        Record record = keyByEmailDao.readWithKey(KeyByEmailColumns.EMAIL, email, KeyByEmailColumns.CREATED, time)
                                     .withConsistency(ConsistencyLevel.QUORUM)
                                     .execute()
                                     .get();
        Assert.assertEquals(id, record.getValue(KeyByEmailColumns.ACCOUNT_ID));
        Assert.assertArrayEquals(key, record.getValue(KeyByEmailColumns.KEY));
        
        record = keyByAccountDao.readWithKey(KeyByAccountColumns.ACCOUNT_ID, id)
                                .withConsistency(ConsistencyLevel.QUORUM)
                                .execute()
                                .get();
        Assert.assertEquals(id, record.getValue(KeyByAccountColumns.ACCOUNT_ID));
        Assert.assertEquals(email, record.getValue(KeyByAccountColumns.EMAIL_IDX).iterator().next().getString(0));
        Assert.assertEquals(time, record.getValue(KeyByAccountColumns.EMAIL_IDX).iterator().next().getLong(1));
        
        
        
        
        
        
        ///////////////////////////////////////////////////////
        // Delete
        try {
            keyByAccountDao.deleteWithKey(KeyByAccountColumns.ACCOUNT_ID, id)
                           .withConsistency(ConsistencyLevel.QUORUM)
                           .ifExists()
                           .execute();
            Assert.fail("InvalidQueryException expected");
        } catch (InvalidQueryException expected) {}        
    
        
    }
    


    
    
    @Test
    public void testCasscadingUnsupportedDeleteQuery() throws Exception {   
        
        DaoManager daoManager = new DaoManager(cassandra.getSession());
       
        Dao keyByAccountDao = daoManager.getKeyByAccountDao();
        Dao keyByEmailDao = daoManager.getKeyByEmailDao();

        
        String id = "act3345445544";
        byte[] key = new byte[] { 34, 56, 87, 88 };
        String email = "me@example.org";
        long time = System.currentTimeMillis(); 
        
        
        //////////////////////////////////////
        // insert 
        // 3.x API change
        //TupleType idxType = TupleType.of(DataType.text(), DataType.bigint());     
        TupleType idxType = TupleType.of(protocolVersion, codecRegistry,DataType.text(), DataType.bigint());
        keyByAccountDao.writeWithKey(KeyByAccountColumns.ACCOUNT_ID, id)
                       .value(KeyByAccountColumns.KEY, key)
                       .addSetValue(KeyByAccountColumns.EMAIL_IDX, idxType.newValue(email, time))
                       .withConsistency(ConsistencyLevel.QUORUM)
                       .execute();
        
        
        
        // test 
        Record record = keyByEmailDao.readWithKey(KeyByEmailColumns.EMAIL, email, KeyByEmailColumns.CREATED, time)
                                     .withConsistency(ConsistencyLevel.QUORUM)
                                     .execute()
                                     .get();
        Assert.assertEquals(id, record.getValue(KeyByEmailColumns.ACCOUNT_ID));
        Assert.assertArrayEquals(key, record.getValue(KeyByEmailColumns.KEY));
        
        record = keyByAccountDao.readWithKey(KeyByAccountColumns.ACCOUNT_ID, id)
                                .withConsistency(ConsistencyLevel.QUORUM)
                                .execute()
                                .get();
        Assert.assertEquals(id, record.getValue(KeyByAccountColumns.ACCOUNT_ID));
        Assert.assertEquals(email, record.getValue(KeyByAccountColumns.EMAIL_IDX).iterator().next().getString(0));
        Assert.assertEquals(time, record.getValue(KeyByAccountColumns.EMAIL_IDX).iterator().next().getLong(1));

        
        
        
        
        
        
        ///////////////////////////////////////////////////////
        // Delete
        
        try {
            keyByAccountDao.deleteWhere(QueryBuilder.in(KeyByAccountColumns.ACCOUNT_ID.getName(), id))
                           .withConsistency(ConsistencyLevel.QUORUM)
                           .execute();
            Assert.fail("InvalidQueryException expected");
        } catch (InvalidQueryException expected) {}        
    }
    
    
    
    
    
    @Test
    public void testCasscadingInsertError() throws Exception {   
       
        Dao keyByAccountDao = new DaoImpl(cassandra.getSession(), KeyByAccountColumns.TABLE).withInterceptor(new ErroneousCascadeOnWriteInterceptor());
       
        
        String id = "act3354455445544";
        byte[] key = new byte[] { 34, 56, 87, 88 };
        String email = "me@example.org";
        long time = System.currentTimeMillis(); 
        
        
        //////////////////////////////////////
        // insert 
        try {
        	// 3.x API change
            //TupleType idxType = TupleType.of(DataType.text(), DataType.bigint());
        	TupleType idxType = TupleType.of(protocolVersion, codecRegistry,DataType.text(), DataType.bigint());
            keyByAccountDao.writeWithKey(KeyByAccountColumns.ACCOUNT_ID, id)
                           .value(KeyByAccountColumns.KEY, key)
                           .addSetValue(KeyByAccountColumns.EMAIL_IDX, idxType.newValue(email, time))
                           .withConsistency(ConsistencyLevel.QUORUM)
                           .execute();
            
            Assert.fail("ClassCastException exepcted");
        } catch (ClassCastException exepcted) { }
    }

    
    
    @Test
    public void testCasscadingDeleteError() throws Exception {   
       
        Dao keyByAccountDao = new DaoImpl(cassandra.getSession(), KeyByAccountColumns.TABLE).withInterceptor(new ErroneousCascadeOnDeleteInterceptor());

        
        String id = "act3343343454544";
        byte[] key = new byte[] { 34, 56, 87, 88 };
        String email = "me@example.org";
        long time = System.currentTimeMillis(); 
        

        //////////////////////////////////////
        // insert 
        // 3.x API change
        //TupleType idxType = TupleType.of(DataType.text(), DataType.bigint());        
        TupleType idxType = TupleType.of(protocolVersion, codecRegistry,DataType.text(), DataType.bigint());
        keyByAccountDao.writeWithKey(KeyByAccountColumns.ACCOUNT_ID, id)
                       .value(KeyByAccountColumns.KEY, key)
                       .addSetValue(KeyByAccountColumns.EMAIL_IDX, idxType.newValue(email, time))
                       .withConsistency(ConsistencyLevel.QUORUM)
                       .execute();
        
        
        
        // test 
        
        Record record = keyByAccountDao.readWithKey(KeyByAccountColumns.ACCOUNT_ID, id)
                                       .withConsistency(ConsistencyLevel.QUORUM)
                                       .execute()
                                       .get();
        Assert.assertEquals(id, record.getValue(KeyByAccountColumns.ACCOUNT_ID));
        Assert.assertEquals(email, record.getValue(KeyByAccountColumns.EMAIL_IDX).iterator().next().getString(0));
        Assert.assertEquals(time, record.getValue(KeyByAccountColumns.EMAIL_IDX).iterator().next().getLong(1));

        
        
        
        
        ///////////////////////////////////////////////////////
        // Delete
        try {
            keyByAccountDao.deleteWithKey(KeyByAccountColumns.ACCOUNT_ID, id)
                           .withConsistency(ConsistencyLevel.QUORUM)
                           .execute();
            
            Assert.fail("ClassCastException exepcted");
        } catch (ClassCastException exepcted) { }
    }

    

    private static final class ErroneousCascadeOnWriteInterceptor implements CascadeOnWriteInterceptor {
        
        @Override
        public CompletableFuture<ImmutableSet<? extends Batchable<?>>> onWrite(WriteQueryData queryData) {
            return CompletableFuture.supplyAsync(() -> { sleep(220); throw new ClassCastException("class cast error"); } );
        }
    }
    
    private static void sleep(int millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException ignore) { }
    }
    
    private static final class ErroneousCascadeOnDeleteInterceptor implements CascadeOnDeleteInterceptor {
    
        @Override
        public CompletableFuture<ImmutableSet<? extends Batchable<?>>> onDelete(DeleteQueryData queryData) {
            return CompletableFuture.supplyAsync(() -> { sleep(220); throw new ClassCastException("class cast error"); } );
        }
    }
}