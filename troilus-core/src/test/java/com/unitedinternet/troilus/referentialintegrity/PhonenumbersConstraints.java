package com.unitedinternet.troilus.referentialintegrity;



import java.util.Optional;








import com.datastax.driver.core.ConsistencyLevel;
import com.unitedinternet.troilus.Dao;
import com.unitedinternet.troilus.Record;
import com.unitedinternet.troilus.interceptor.SingleReadQueryData;
import com.unitedinternet.troilus.interceptor.SingleReadQueryPreInterceptor;
import com.unitedinternet.troilus.interceptor.SingleReadQueryPostInterceptor;
import com.unitedinternet.troilus.interceptor.WriteQueryData;
import com.unitedinternet.troilus.interceptor.WriteQueryPreInterceptor;
import com.google.common.collect.ImmutableSet;




class PhonenumbersConstraints implements WriteQueryPreInterceptor, 
                                         SingleReadQueryPreInterceptor,
                                         SingleReadQueryPostInterceptor {
    

    private final Dao deviceDao;
    
    public PhonenumbersConstraints(Dao deviceDao) {
        this.deviceDao = deviceDao.withConsistency(ConsistencyLevel.QUORUM);
    }
        

    
    
    @Override
    public WriteQueryData onPreWrite(WriteQueryData data) {
        
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
    public SingleReadQueryData onPreSingleRead(SingleReadQueryData data) {
        // force that device_id will be fetched 
        if (data.getColumnsToFetch().isPresent() && !data.getColumnsToFetch().get().containsKey("device_id")) {
            data = data.columnsToFetch(Immutables.merge(data.getColumnsToFetch(), "device_id", false));
        }
        return data;
    }
    
    
    
    @Override
    public Optional<Record> onPostSingleRead(SingleReadQueryData data, Optional<Record> optionalRecord) {
        String number = (String) data.getKeyNameValuePairs().get("number");
        
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


