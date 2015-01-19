package net.oneandone.troilus.referentialintegrity;



import java.util.Optional;


















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
    public WriteQueryData onWriteRequest(WriteQueryData data) {
        
        // unique insert?
        if (data.getIfNotExits().isPresent() && data.getIfNotExits().get()) {
            ConstraintException.throwIf(!data.getValuesToMutate().containsKey("device_id"), "columnn 'device_id' is mandatory");
            
            String deviceId = (String) data.getValuesToMutate().get("device_id").get();
            ConstraintException.throwIf(!deviceDao.readWithKey("device_id", deviceId).execute().isPresent(), "device with id " + deviceId + " does not exits");                                                                                    
            
        // no, update
        } else {
            ConstraintException.throwIf(data.getValuesToMutate().containsKey("device_id"), "columnn 'device_id' is unmodifiable");
        }
           
        return data; 
    }


    
    @Override
    public SingleReadQueryData onSingleReadRequest(SingleReadQueryData data) {
        // force that device_id will be fetched 
        if (!data.getColumnsToFetch().isEmpty() && !data.getColumnsToFetch().containsKey("device_id")) {
            data = data.columnsToFetch(Immutables.merge(data.getColumnsToFetch(), "device_id", false));
        }
        return data;
    }
    
    
    
    @Override
    public Optional<Record> onSingleReadResponse(SingleReadQueryData data, Optional<Record> optionalRecord) {
        String number = (String) data.getKey().get("number");
        
        if (optionalRecord.isPresent() && optionalRecord.get().getString("device_id").isPresent()) {
            
            String deviceId = optionalRecord.get().getString("device_id").get();
            if (deviceId != null) {
                deviceDao.readWithKey("device_id", deviceId)
                         .column("phone_numbers")
                         .withConsistency(ConsistencyLevel.ONE)
                         .execute()
                         .ifPresent(rec -> {
                                             Optional<ImmutableSet<String>> set = rec.getSet("phone_numbers", String.class);
                                             ConstraintException.throwIf(!set.isPresent() || !set.get().contains(number), "reverse reference devices table -> phone_numbers table does not exit");
                                          });
            }
            
        }
        
        return optionalRecord;
    }
}


