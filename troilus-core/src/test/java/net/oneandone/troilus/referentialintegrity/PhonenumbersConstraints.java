package net.oneandone.troilus.referentialintegrity;



import java.util.Optional;



















import java.util.concurrent.CompletableFuture;

import net.oneandone.troilus.Dao;
import net.oneandone.troilus.Record;
import net.oneandone.troilus.interceptor.SingleReadQueryData;
import net.oneandone.troilus.interceptor.SingleReadQueryRequestInterceptor;
import net.oneandone.troilus.interceptor.SingleReadQueryResponseInterceptor;
import net.oneandone.troilus.interceptor.WriteQueryData;
import net.oneandone.troilus.interceptor.WriteQueryRequestInterceptor;

import com.datastax.driver.core.ConsistencyLevel;
import com.google.common.collect.ImmutableSet;

    


class PhonenumbersConstraints implements WriteQueryRequestInterceptor, 
                                         SingleReadQueryRequestInterceptor,
                                         SingleReadQueryResponseInterceptor {
    

    private final Dao deviceDao;
    
    public PhonenumbersConstraints(Dao deviceDao) {
       // this.deviceDao = deviceDao.withConsistency(ConsistencyLevel.QUORUM);
        this.deviceDao = deviceDao;
    }
        

    
    @Override
    public CompletableFuture<WriteQueryData> onWriteRequestAsync( WriteQueryData queryData) {
        
        // unique insert?
        if (queryData.getIfNotExits().isPresent() && queryData.getIfNotExits().get()) {
            ConstraintException.throwIf(!queryData.getValuesToMutate().containsKey("device_id"), "columnn 'device_id' is mandatory");
            
            String deviceId = (String) queryData.getValuesToMutate().get("device_id").get();
            ConstraintException.throwIf(!deviceDao.readWithKey("device_id", deviceId).execute().isPresent(), "device with id " + deviceId + " does not exits");                                                                                    
            
        // no, update
        } else {
            ConstraintException.throwIf(queryData.getValuesToMutate().containsKey("device_id"), "columnn 'device_id' is unmodifiable");
        }
           
        return CompletableFuture.completedFuture(queryData); 
    }

    

    @Override
    public CompletableFuture<SingleReadQueryData> onSingleReadRequestAsync( SingleReadQueryData queryData) {
        // force that device_id will be fetched 
        if (!queryData.getColumnsToFetch().isEmpty() && !queryData.getColumnsToFetch().containsKey("device_id")) {
            queryData = queryData.columnsToFetch(Immutables.merge(queryData.getColumnsToFetch(), "device_id", false));
        }
        return CompletableFuture.completedFuture(queryData);
    }
    
    
    @Override
    public CompletableFuture<Optional<Record>> onSingleReadResponseAsync(SingleReadQueryData queryData, Optional<Record> optionalRecord) {

        if (optionalRecord.isPresent() && optionalRecord.get().getString("device_id").isPresent()) {
            return deviceDao.readWithKey("device_id", optionalRecord.get().getString("device_id").get())
                            .column("phone_numbers")
                            .withConsistency(ConsistencyLevel.ONE)
                            .executeAsync()
                            .thenApply(optionalRec -> {
                                                        optionalRec.ifPresent(rec -> {
                                                            Optional<ImmutableSet<String>> set = rec.getSet("phone_numbers", String.class);
                                                            ConstraintException.throwIf(!set.isPresent() || !set.get().contains(queryData.getKey().get("number")), "reverse reference devices table -> phone_numbers table does not exit");
                                                        });
                                                        
                                                        return optionalRecord;
                                                      });
            
        } else {
            return CompletableFuture.completedFuture(optionalRecord);
        }
    }
    
}


