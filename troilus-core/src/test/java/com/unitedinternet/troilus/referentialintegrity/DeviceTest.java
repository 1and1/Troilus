package com.unitedinternet.troilus.referentialintegrity;



import java.util.Optional;

import org.junit.Assert;
import org.junit.Test;










import com.unitedinternet.troilus.AbstractCassandraBasedTest;
import com.unitedinternet.troilus.Dao;
import com.unitedinternet.troilus.DaoImpl;
import com.unitedinternet.troilus.Record;
import com.unitedinternet.troilus.interceptor.DeleteQueryData;
import com.unitedinternet.troilus.interceptor.DeleteQueryPreInterceptor;
import com.unitedinternet.troilus.interceptor.InsertQueryData;
import com.unitedinternet.troilus.interceptor.SingleReadQueryData;
import com.unitedinternet.troilus.interceptor.UpdateQueryData;
import com.unitedinternet.troilus.interceptor.InsertQueryPreInterceptor;
import com.unitedinternet.troilus.interceptor.SingleReadQueryPostInterceptor;
import com.unitedinternet.troilus.interceptor.UpdateQueryPreInterceptor;



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
 
       
       
    
    
    private static final class PhonenumbersConstraints implements UpdateQueryPreInterceptor, InsertQueryPreInterceptor, DeleteQueryPreInterceptor, SingleReadQueryPostInterceptor {

        private final Dao deviceDao;
        
        public PhonenumbersConstraints(Dao deviceDao) {
            this.deviceDao = deviceDao;
        }
            
        @Override
        public InsertQueryData onPreInsert(InsertQueryData data) {
            data.getValuesToMutate().get("device_id").ifPresent(deviceid -> { /* check if device exits */ });
            
            return data;
        }
    

        @Override
        public DeleteQueryData onPreDelete(DeleteQueryData data) {
            /* check that the device not exists */
            return null;
        }
        
        
        @Override
        public UpdateQueryData onPreUpdate(UpdateQueryData data) {

            if (data.getValuesToMutate().containsKey("device_id")) {
                throw new ConstraintException("columnn 'device_id' is unmodifiable");
            }
               
            return data; 
        }


        
        @Override
        public Optional<Record> onPostSingleRead(SingleReadQueryData data, Optional<Record> record) {
            // check is related device includes this number
            return record;
        }
    }
    
    
    

    private static final class DeviceConstraints implements SingleReadQueryPostInterceptor {
        private final Dao phoneNumbersDao;
                    
        public DeviceConstraints(Dao phoneNumbersDao) {
            this.phoneNumbersDao = phoneNumbersDao;
        }
        
        @Override
        public Optional<Record> onPostSingleRead(SingleReadQueryData data, Optional<Record> record) {
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


