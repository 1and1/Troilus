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




import java.nio.ByteBuffer;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;

import net.oneandone.troilus.AbstractCassandraBasedTest;
import net.oneandone.troilus.Dao;
import net.oneandone.troilus.DaoImpl;
import net.oneandone.troilus.interceptor.DeleteQueryData;
import net.oneandone.troilus.interceptor.DeleteQueryRequestInterceptor;
import net.oneandone.troilus.interceptor.WriteQueryData;
import net.oneandone.troilus.interceptor.WriteQueryRequestInterceptor;
import net.oneandone.troilus.persistence.User;

import org.junit.Assert;
import org.junit.Test;


public class InterceptorTest extends AbstractCassandraBasedTest {
    

        
    
    @Test
    public void testInterceptor() throws Exception {
        MyWriteQueryRequestInterceptor writeRequestInterceptor = new MyWriteQueryRequestInterceptor(); 
        MyDeleteQueryRequestInterceptor deleteRequestInterceptor = new MyDeleteQueryRequestInterceptor(); 
        
        Dao usersDao = new DaoImpl(getSession(), UsersTable.TABLE)
                                 .withInterceptor(writeRequestInterceptor)
                                 .withInterceptor(deleteRequestInterceptor);
        
        usersDao.writeEntity(new User("34334234234", "tom", false, ByteBuffer.allocate(0), new byte[0], System.currentTimeMillis(), null, null))
                .execute();
        Assert.assertEquals("tom", writeRequestInterceptor.getQueryData().getValuesToMutate().get("name").get());

        
        
        usersDao.deleteWithKey("user_id", "34334234234")
                .execute();
        Assert.assertEquals("34334234234", deleteRequestInterceptor.getQueryData().getKeys().get("user_id"));
    }
    
    
    
    private static final class MyWriteQueryRequestInterceptor implements WriteQueryRequestInterceptor {
        
        private AtomicReference<WriteQueryData> queryDataRef = new AtomicReference<>();

        @Override
        public CompletableFuture<WriteQueryData> onWriteRequestAsync(WriteQueryData queryData) {
            this.queryDataRef.set(queryData);
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
}


