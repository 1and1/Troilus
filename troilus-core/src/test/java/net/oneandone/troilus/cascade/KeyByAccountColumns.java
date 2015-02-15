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

import java.util.List;
import java.util.Map;


import java.util.Map.Entry;
import java.util.concurrent.CompletableFuture;

import com.datastax.driver.core.ConsistencyLevel;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;

import net.oneandone.troilus.Batchable;
import net.oneandone.troilus.ColumnName;
import net.oneandone.troilus.Dao;
import net.oneandone.troilus.Deletion;
import net.oneandone.troilus.Record;
import net.oneandone.troilus.Schema;
import net.oneandone.troilus.Write;
import net.oneandone.troilus.interceptor.DeleteQueryData;
import net.oneandone.troilus.interceptor.WriteQueryData;
import net.oneandone.troilus.interceptor.CascadeOnDeleteInterceptor;
import net.oneandone.troilus.interceptor.CascadeOnWriteInterceptor;



public interface KeyByAccountColumns  {
   
    public static final String TABLE = "key_by_accountid";

    public static final ColumnName<String> ACCOUNT_ID = ColumnName.defineString("account_id");
    public static final ColumnName<byte[]> KEY = ColumnName.defineBytes("key");
    public static final ColumnName<Map<String, Long>> EMAIL_IDX = ColumnName.defineMap("email_idx", String.class, Long.class);
    
    public static final String CREATE_STMT = Schema.load("com/unitedinternet/troilus/example/key_by_accountid.ddl");
    
    
    
    public static final class CascadeByEmailDao implements CascadeOnWriteInterceptor, CascadeOnDeleteInterceptor {
        private final Dao keyByAccountDao;
        private final Dao keyByEmailDao;
        
        public CascadeByEmailDao(Dao keyByAccountDao, Dao keyByEmailDao) {
            this.keyByAccountDao = keyByAccountDao;
            this.keyByEmailDao = keyByEmailDao;
        }

        @Override
        public CompletableFuture<ImmutableSet<? extends Batchable>> onWrite(WriteQueryData queryData) {
            Map<String, Long> fk = (Map<String, Long>) queryData.getValuesToMutate().get(EMAIL_IDX.getName()).get();
            
            List<Write> writes = Lists.newArrayList();
            for (Entry<String, Long> entry : fk.entrySet()) {
                writes.add(keyByEmailDao.writeWithKey(KeyByEmailColumns.EMAIL, entry.getKey(), KeyByEmailColumns.CREATED, entry.getValue())
                                        .value(KeyByEmailColumns.KEY, (byte[]) queryData.getValuesToMutate().get(KEY.getName()).get())
                                        .value(KeyByEmailColumns.ACCOUNT_ID, (String) queryData.getKeys().get(ACCOUNT_ID.getName()))
                                        .withConsistency(ConsistencyLevel.QUORUM));
            }
            
            return CompletableFuture.completedFuture(ImmutableSet.copyOf(writes));
        }
        
        
        @Override
        public CompletableFuture<ImmutableSet<? extends Batchable>> onDelete(DeleteQueryData queryData) {
            // resolve dependent records
            return keyByAccountDao.readWithKey(queryData.getKey())
                                  .withConsistency(ConsistencyLevel.QUORUM)
                                  .executeAsync()
                                  .thenApply(optionalRecord -> optionalRecord.map(record -> getDeletions(record)).orElse(ImmutableSet.of()));
        }
        
        
        private ImmutableSet<Deletion> getDeletions(Record record) {
            List<Deletion> deletions = Lists.newArrayList();
            for (Entry<String, Long> entry : record.getValue(KeyByAccountColumns.EMAIL_IDX).entrySet()) {
                deletions.add(keyByEmailDao.deleteWithKey(KeyByEmailColumns.EMAIL, entry.getKey(), KeyByEmailColumns.CREATED, entry.getValue())
                                           .withConsistency(ConsistencyLevel.QUORUM));
            }
            
            return ImmutableSet.copyOf(deletions);
        }
    }
}
