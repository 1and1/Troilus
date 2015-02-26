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



import java.util.Optional;



import java.util.concurrent.CompletableFuture;

import net.oneandone.troilus.ConstraintException;
import net.oneandone.troilus.Dao;
import net.oneandone.troilus.Record;
import net.oneandone.troilus.interceptor.SingleReadQueryData;
import net.oneandone.troilus.interceptor.SingleReadQueryRequestInterceptor;
import net.oneandone.troilus.interceptor.SingleReadQueryResponseInterceptor;

import com.datastax.driver.core.ConsistencyLevel;
import com.google.common.collect.ImmutableSet;
    


class PhonenumbersConstraints implements SingleReadQueryRequestInterceptor,
                                         SingleReadQueryResponseInterceptor {
    

    private final Dao deviceDao;
    
    public PhonenumbersConstraints(Dao deviceDao) {
        this.deviceDao = deviceDao.withConsistency(ConsistencyLevel.QUORUM);
    }
        
    
    @Override
    public CompletableFuture<SingleReadQueryData> onSingleReadRequestAsync( SingleReadQueryData queryData) {
        // force that device_id will be fetched 
        if (!queryData.getColumnsToFetch().containsKey("device_id")) {
            queryData = queryData.columnsToFetch(Immutables.merge(queryData.getColumnsToFetch(), "device_id", false));
        }
        return CompletableFuture.completedFuture(queryData);
    }
    
    
    @Override
    public CompletableFuture<Optional<Record>> onSingleReadResponseAsync(SingleReadQueryData queryData, Optional<Record> optionalRecord) {


        if (optionalRecord.isPresent() && (optionalRecord.get().getString("device_id") != null)) {
System.out.println("cross check dao call");
            
            return deviceDao.readWithKey("device_id", optionalRecord.get().getString("device_id"))
                            .column("phone_numbers")
                            .withConsistency(ConsistencyLevel.ONE)
                            .executeAsync()
                            .thenApply(optionalRec -> {
                                System.out.println("cross check dao call done");
                                                        optionalRec.ifPresent(rec -> {
                                                            ImmutableSet<String> set = rec.getSet("phone_numbers", String.class);
                                                            if (!set.isEmpty() && !set.contains(queryData.getKey().get("number"))) {
                                                                throw new ConstraintException("reverse reference devices table -> phone_numbers table does not exit");
                                                            }
                                                        });
                                                        
                                                        return optionalRecord;
                                                      });
            
        } else {
            return CompletableFuture.completedFuture(optionalRecord);
        }
    }
}


