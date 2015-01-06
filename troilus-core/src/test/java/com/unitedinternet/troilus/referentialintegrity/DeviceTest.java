package com.unitedinternet.troilus.referentialintegrity;



import java.util.Optional;

import org.junit.Assert;
import org.junit.Test;


















import com.datastax.driver.core.ConsistencyLevel;
import com.unitedinternet.troilus.AbstractCassandraBasedTest;
import com.unitedinternet.troilus.Dao;
import com.unitedinternet.troilus.DaoImpl;
import com.unitedinternet.troilus.InsertQueryBeforeInterceptor;
import com.unitedinternet.troilus.InsertQueryData;
import com.unitedinternet.troilus.Record;
import com.unitedinternet.troilus.SingleReadQueryAfterInterceptor;
import com.unitedinternet.troilus.SingleReadQueryData;
import com.unitedinternet.troilus.UpdateQueryBeforeInterceptor;
import com.unitedinternet.troilus.UpdateQueryData;



public class DeviceTest extends AbstractCassandraBasedTest {
    
    

    @Test
    public void testUnmodifyableColumn() throws Exception {
                
        Dao phoneNumbersDao = new DaoImpl(getSession(), PhonenumbersTable.TABLE)
                                   .withConsistency(ConsistencyLevel.LOCAL_QUORUM)
                                   .withSerialConsistency(ConsistencyLevel.SERIAL);


        phoneNumbersDao.writeWithKey(PhonenumbersTable.NUMBER, "0089645454455")
                       .value(PhonenumbersTable.DEVICE_ID, "deaeea")
                       .value(PhonenumbersTable.ACTIVE, true)
                       .ifNotExits()
                       .execute();
        
        
        phoneNumbersDao.writeWithKey(PhonenumbersTable.NUMBER, "0089123234234")
                       .value(PhonenumbersTable.DEVICE_ID, "dfacbsd")
                       .value(PhonenumbersTable.ACTIVE, true)
                       .ifNotExits()
                       .execute();
        
        
        // with unmodifiyable constraint
        Dao phoneNumbersDaoWithConstraints = phoneNumbersDao.withInterceptor(new PhonenumbersUnmodifiableColumnConstraint());
        
        
        // update modifyable column
        phoneNumbersDaoWithConstraints.writeWithKey(PhonenumbersTable.NUMBER, "0089123234234")
                                      .value(PhonenumbersTable.ACTIVE, false)
                                      .execute();


        // update non-modifyable column
        try {
            phoneNumbersDaoWithConstraints.writeWithKey(PhonenumbersTable.NUMBER, "0089123234234")
                                          .value(PhonenumbersTable.DEVICE_ID, "dfacbsd")
                                          .execute();
            Assert.fail("RuntimeException expected");
        } catch (RuntimeException expected) {  }
    }
    
    
    
    
       @Test
    public void testRI() throws Exception {
                
        Dao phoneNumbersDao = new DaoImpl(getSession(), PhonenumbersTable.TABLE);
        Dao deviceDao = new DaoImpl(getSession(), DeviceTable.TABLE);
        
        Dao phoneNumbersDaoWithConstraints = phoneNumbersDao.withInterceptor(new PhonenumbersConstraints(deviceDao));
        Dao deviceDaoWithConstraints = deviceDao.withInterceptor(new DeviceConstraints(phoneNumbersDao));


        
        deviceDaoWithConstraints.writeWithKey(DeviceTable.ID, "834343")
                                .value(DeviceTable.TYPE, 3)
                                .ifNotExits()
                                .execute();
        
        
        deviceDaoWithConstraints.writeWithKey(DeviceTable.ID, "2333243")
                                .value(DeviceTable.TYPE, 1)
                                .ifNotExits()
                                .execute();

        
        deviceDaoWithConstraints.writeWithKey(DeviceTable.ID, "934453434")
                                .value(DeviceTable.TYPE, 3)
                                .ifNotExits()
                                .execute();
        
        
        
        
    }       
 
       
    private static final class PhonenumbersUnmodifiableColumnConstraint implements UpdateQueryBeforeInterceptor {
           
        @Override
        public UpdateQueryData onBeforeUpdate(UpdateQueryData data) {

            if (data.getValuesToMutate().containsKey(PhonenumbersTable.DEVICE_ID)) {
                throw new ConstraintException("columnn " + PhonenumbersTable.DEVICE_ID + " is unmodifiable");
            }
               
            return data; 
        }
    }
       
       
    
    private static final class PhonenumbersConstraints implements UpdateQueryBeforeInterceptor, InsertQueryBeforeInterceptor, SingleReadQueryAfterInterceptor {

        private final Dao deviceDao;
        
        public PhonenumbersConstraints(Dao deviceDao) {
            this.deviceDao = deviceDao;
        }
            
        @Override
        public InsertQueryData onBeforeInsert(InsertQueryData data) {
            data.getValuesToMutate().get(PhonenumbersTable.DEVICE_ID).ifPresent(deviceid -> { /* check if device exits */ });
            
            return data;
        }
    
        
        
        @Override
        public UpdateQueryData onBeforeUpdate(UpdateQueryData data) {

            if (data.getValuesToMutate().containsKey(PhonenumbersTable.DEVICE_ID)) {
                throw new ConstraintException("columnn " + PhonenumbersTable.DEVICE_ID + " is unmodifiable");
            }
               
            return data; 
        }


        
        @Override
        public Optional<Record> onAfterSingleRead(SingleReadQueryData data, Optional<Record> record) {
            // check is related device includes this number
            return record;
        }
    }
    
    
    

    private static final class DeviceConstraints implements SingleReadQueryAfterInterceptor {
        private final Dao phoneNumbersDao;
                    
        public DeviceConstraints(Dao phoneNumbersDao) {
            this.phoneNumbersDao = phoneNumbersDao;
        }
        
        @Override
        public Optional<Record> onAfterSingleRead(SingleReadQueryData data, Optional<Record> record) {
            // check is related phone numbers points to this device
            return record;
        }
    }
    
    
    

    private static final class ConstraintException extends RuntimeException {
        private static final long serialVersionUID = 9207265892679971373L;

        public ConstraintException(String message) {
            super(message);
        }
    }

}


