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
package net.oneandone.troilus.referentialintegrity;





import java.util.Iterator;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import net.oneandone.troilus.Dao;
import net.oneandone.troilus.Record;
import net.oneandone.troilus.RecordList;
import net.oneandone.troilus.ConstraintException;
import net.oneandone.troilus.interceptor.ReadQueryData;
import net.oneandone.troilus.interceptor.ReadQueryRequestInterceptor;
import net.oneandone.troilus.interceptor.ReadQueryResponseInterceptor;
import net.oneandone.troilus.interceptor.RecordListAdapter;

import com.datastax.driver.core.ConsistencyLevel;
import com.google.common.collect.ImmutableSet;
    


class PhonenumbersConstraints implements ReadQueryRequestInterceptor,
                                         ReadQueryResponseInterceptor {
    

    private final Dao deviceDao;
    
    public PhonenumbersConstraints(Dao deviceDao) {
        this.deviceDao = deviceDao.withConsistency(ConsistencyLevel.QUORUM);
    }
        
    
    @Override
    public CompletableFuture<ReadQueryData> onReadRequestAsync(ReadQueryData queryData) {
        // force that device_id will be fetched 
        if (!queryData.getColumnsToFetch().containsKey("device_id")) {
            queryData = queryData.columnsToFetch(Immutables.merge(queryData.getColumnsToFetch(), "device_id", false));
        }
        return CompletableFuture.completedFuture(queryData);
    }
    

    @Override
    public CompletableFuture<RecordList> onReadResponseAsync(ReadQueryData queryData, RecordList recordList) {
        return CompletableFuture.completedFuture(new VaildatingRecordList(recordList, deviceDao));
    }
    
    
    private static final class VaildatingRecordList extends RecordListAdapter {
     
        private final Dao deviceDao;

        
        public VaildatingRecordList(RecordList recordList, Dao deviceDao) {
            super(recordList);
            this.deviceDao = deviceDao;
        }
        
        @Override
        public Iterator<Record> iterator() {
            return new ValidatingIterator(super.iterator());
        }

        
        private final class ValidatingIterator implements Iterator<Record> {
            private Iterator<Record> it;
            
            public ValidatingIterator(Iterator<Record> it) {
                this.it = it;
            }
            
            @Override
            public boolean hasNext() {
                return it.hasNext();
            }
            
            
            @Override
            public Record next() {
                
                Record record = it.next();
                
                Optional<Record> deviceRecord = deviceDao.readWithKey("device_id", record.getString("device_id"))
                                                         .column("phone_numbers")
                                                         .withConsistency(ConsistencyLevel.ONE)
                                                         .execute();
                
                deviceRecord.ifPresent(rec -> {
                                                ImmutableSet<String> set = rec.getSet("phone_numbers", String.class);
                                                if (!set.isEmpty() && !set.contains(record.getString("number"))) {
                                                    throw new ConstraintException("reverse reference devices table -> phone_numbers table does not exit");
                                                }
                                              });
                
                return record;
            }
        }
    }
}


