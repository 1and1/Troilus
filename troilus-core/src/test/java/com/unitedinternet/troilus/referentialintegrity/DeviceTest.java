package com.unitedinternet.troilus.referentialintegrity;



import java.util.Optional;

import org.junit.Assert;
import org.junit.Test;


import com.unitedinternet.troilus.AbstractCassandraBasedTest;
import com.unitedinternet.troilus.Dao;
import com.unitedinternet.troilus.DaoImpl;
import com.unitedinternet.troilus.DeleteQueryBeforeInterceptor;
import com.unitedinternet.troilus.DeleteQueryData;
import com.unitedinternet.troilus.InsertQueryBeforeInterceptor;
import com.unitedinternet.troilus.InsertQueryData;
import com.unitedinternet.troilus.Record;
import com.unitedinternet.troilus.SingleReadQueryAfterInterceptor;
import com.unitedinternet.troilus.SingleReadQueryData;
import com.unitedinternet.troilus.UpdateQueryBeforeInterceptor;
import com.unitedinternet.troilus.UpdateQueryData;



public class DeviceTest extends AbstractCassandraBasedTest {
    
    

    
       @Test
    public void testRI() throws Exception {
           
           
        /*
            The phones table is used to assign a phone number to a device. The key is the phone number. The phone number table contains a device id column referring to the assigned device. The
            1, insert operations ensures that an existing phone row will not be overridden. 
            2, it should not be allowed to update the device id column. This means assigning a phone number to new devices requires to remove the old entry first and to create a new phones row.
            3, A phone number can not be assigned to a non-existing device. 
            4, A phone number will not be deleted, if the assigned device still exits
            5, by accessing the table entries the back relation should be check with cl one 
         */
                
        Dao phoneNumbersDao = new DaoImpl(getSession(), "phone_numbers");
        Dao deviceDao = new DaoImpl(getSession(), "device");
        
        Dao phoneNumbersDaoWithConstraints = phoneNumbersDao.withInterceptor(new PhonenumbersConstraints(deviceDao));
        Dao deviceDaoWithConstraints = deviceDao.withInterceptor(new DeviceConstraints(phoneNumbersDao));


        
        deviceDaoWithConstraints.writeWithKey("device_id", "834343")
                                .value(DeviceTable.TYPE, 3)
                                .ifNotExits()
                                .execute();
        
        
        deviceDaoWithConstraints.writeWithKey("device_id", "2333243")
                                .value(DeviceTable.TYPE, 1)
                                .ifNotExits()
                                .execute();

        
        deviceDaoWithConstraints.writeWithKey("device_id", "934453434")
                                .value(DeviceTable.TYPE, 3)
                                .ifNotExits()
                                .execute();
        
    
        
        
        
        
        
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
 
       
       
    
    
    private static final class PhonenumbersConstraints implements UpdateQueryBeforeInterceptor, InsertQueryBeforeInterceptor, DeleteQueryBeforeInterceptor, SingleReadQueryAfterInterceptor {

        private final Dao deviceDao;
        
        public PhonenumbersConstraints(Dao deviceDao) {
            this.deviceDao = deviceDao;
        }
            
        @Override
        public InsertQueryData onBeforeInsert(InsertQueryData data) {
            data.getValuesToMutate().get("device_id").ifPresent(deviceid -> { /* check if device exits */ });
            
            return data;
        }
    

        @Override
        public DeleteQueryData onBeforeDelete(DeleteQueryData data) {
            /* check that the device not exists */
            return null;
        }
        
        
        @Override
        public UpdateQueryData onBeforeUpdate(UpdateQueryData data) {

            if (data.getValuesToMutate().containsKey("device_id")) {
                throw new ConstraintException("columnn 'device_id' is unmodifiable");
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


