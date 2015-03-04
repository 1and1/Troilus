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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;

import net.oneandone.troilus.CassandraDB;
import net.oneandone.troilus.Dao;
import net.oneandone.troilus.DaoImpl;
import net.oneandone.troilus.Record;
import net.oneandone.troilus.ResultList;
import net.oneandone.troilus.interceptor.DeleteQueryData;
import net.oneandone.troilus.interceptor.DeleteQueryRequestInterceptor;
import net.oneandone.troilus.interceptor.ReadQueryData;
import net.oneandone.troilus.interceptor.ReadQueryRequestInterceptor;
import net.oneandone.troilus.interceptor.ReadQueryResponseInterceptor;
import net.oneandone.troilus.interceptor.WriteQueryData;
import net.oneandone.troilus.interceptor.WriteQueryRequestInterceptor;
import net.oneandone.troilus.persistence.User;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.datastax.driver.core.querybuilder.QueryBuilder;


public class InterceptorTest  {

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
        cassandra.tryExecuteCqlFile(UsersTable.DDL);
    }

        
    
    @Test
    public void testInterceptor() throws Exception {
        MyWriteQueryRequestInterceptor writeRequestInterceptor = new MyWriteQueryRequestInterceptor(); 
        MyListReadQueryRequestInterceptor listReadRequestInterceptor = new MyListReadQueryRequestInterceptor();
        MyListReadQueryResponseInterceptor listReadResponseInterceptor = new MyListReadQueryResponseInterceptor();
        MyDeleteQueryRequestInterceptor deleteRequestInterceptor = new MyDeleteQueryRequestInterceptor(); 
        
        Dao usersDao = new DaoImpl(cassandra.getSession(), UsersTable.TABLE)
                                 .withInterceptor(writeRequestInterceptor)
                                 .withInterceptor(deleteRequestInterceptor)
                                 .withInterceptor(listReadRequestInterceptor)
                                 .withInterceptor(listReadResponseInterceptor);
        
        
        String s = usersDao.toString();
        Assert.assertTrue(s.contains("ListReadQueryPostInterceptor (with"));
         
        usersDao.writeEntity(new User("34334234234", "tom", false, ByteBuffer.allocate(0), new byte[0], System.currentTimeMillis(), null, null))
                .execute();
        Assert.assertEquals("tom", writeRequestInterceptor.getQueryData().getValuesToMutate().get("name").get());


        
        usersDao.readSequenceWhere(QueryBuilder.in("user_id", "34334234234"))
                .execute();
        Assert.assertTrue(listReadRequestInterceptor.getQueryData().getWhereConditions().size() > 0);
        Assert.assertEquals("tom", listReadResponseInterceptor.getRecord().iterator().next().getString("name"));


        
        usersDao.deleteWithKey("user_id", "34334234234")
                .execute();
        Assert.assertEquals("34334234234", deleteRequestInterceptor.getQueryData().getKey().get("user_id"));
    }
    
    
    
    private static final class MyWriteQueryRequestInterceptor implements WriteQueryRequestInterceptor {
        
        private AtomicReference<WriteQueryData> queryDataRef = new AtomicReference<>();

        @Override
        public CompletableFuture<WriteQueryData> onWriteRequestAsync(WriteQueryData queryData) {
            this.queryDataRef.set(queryData);
            
            queryData = queryData.keys(queryData.getKeys())
                                 .valuesToMutate(queryData.getValuesToMutate())
                                 .listValuesToAppend(queryData.getListValuesToAppend())
                                 .listValuesToPrepend(queryData.getListValuesToPrepend())
                                 .listValuesToRemove(queryData.getListValuesToRemove())
                                 .mapValuesToMutate(queryData.getMapValuesToMutate())
                                 .ifNotExists(queryData.getIfNotExits())
                                 .onlyIfConditions(queryData.getOnlyIfConditions());
                    
            return CompletableFuture.completedFuture(queryData);
        }
        
        public WriteQueryData getQueryData() {
            return queryDataRef.get();
        }
    }
    
    
    private static final class MyDeleteQueryRequestInterceptor implements DeleteQueryRequestInterceptor {
        
        private AtomicReference<DeleteQueryData> queryDataRef = new AtomicReference<>();


        @Override
        public CompletableFuture<DeleteQueryData> onDeleteRequestAsync( DeleteQueryData queryData) {
            this.queryDataRef.set(queryData);
            return CompletableFuture.completedFuture(queryData);
        }
        
        public DeleteQueryData getQueryData() {
            return queryDataRef.get();
        }
    }
    
        
    
    private static final class MyListReadQueryRequestInterceptor implements ReadQueryRequestInterceptor {
        
        private AtomicReference<ReadQueryData> queryDataRef = new AtomicReference<>();

        @Override
        public CompletableFuture<ReadQueryData> onReadRequestAsync(ReadQueryData queryData) {
            this.queryDataRef.set(queryData);
            
            queryData = queryData.keys(queryData.getKeys())
                                 .columnsToFetch(queryData.getColumnsToFetch())
                                 .whereConditions(queryData.getWhereConditions())
                                 .limit(queryData.getLimit())
                                 .distinct(queryData.getDistinct())
                                 .allowFiltering(queryData.getAllowFiltering());
            
            return CompletableFuture.completedFuture(queryData);
        }
        
        public ReadQueryData getQueryData() {
            return queryDataRef.get();
        }
    }
    
    
    private static final class MyListReadQueryResponseInterceptor implements ReadQueryResponseInterceptor {
        
        private AtomicReference<ResultList<Record>> recordListRef = new AtomicReference<>();

        @Override
        public CompletableFuture<ResultList<Record>> onReadResponseAsync(ReadQueryData queryData, ResultList<Record> recordList) {
            this.recordListRef.set(recordList);
            return CompletableFuture.completedFuture(recordList);
        }
        
        public ResultList<Record> getRecord() {
            return recordListRef.get();
        }
    }
}


