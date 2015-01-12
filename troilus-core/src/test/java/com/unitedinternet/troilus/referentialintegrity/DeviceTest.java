package com.unitedinternet.troilus.referentialintegrity;



import java.util.Optional;

import org.junit.Assert;
import org.junit.Test;

import com.unitedinternet.troilus.AbstractCassandraBasedTest;
import com.unitedinternet.troilus.Dao;
import com.unitedinternet.troilus.DaoImpl;
import com.unitedinternet.troilus.Record;
import com.unitedinternet.troilus.SingleReadQueryData;
import com.unitedinternet.troilus.interceptor.SingleReadQueryPostInterceptor;



public class DeviceTest extends AbstractCassandraBasedTest {
    
    

    
       @Test
    public void testRI() throws Exception {           
           
        /*
            The phones table is used to assign a phone number to a device. The key is the phone number. The phone number table contains a mandatory device id column referring to the assigned device. The
            1, insert operations ensures that an existing phone row will not be overridden. 
            2, it should not be allowed to update the device id column. This means assigning a phone number to new devices requires to remove the old entry first and to create a new phones row.
            3, A phone number can not be assigned to a non-existing device. 
            4, by accessing the table entries the back relation should be check with cl one 
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
                       .value(PhonenumbersTable.DEVICE_ID, "834343")
                       .value(PhonenumbersTable.ACTIVE, true)
                       .ifNotExits()
                       .execute();

        deviceDaoWithConstraints.writeWithKey("device_id", "834343")
                                .addSetValue("phone_numbers", "0089645454455")
                                .execute();
        
        
        
        
        // insert new  entry
        phoneNumbersDao.writeWithKey(PhonenumbersTable.NUMBER, "0089123234234")
                       .value(PhonenumbersTable.DEVICE_ID, "2333243")
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
            Assert.fail("ConstraintException expected");
        } catch (ConstraintException expected) { 
            Assert.assertTrue(expected.getMessage().contains("columnn 'device_id' is unmodifiable"));
        }
        
        
  
        // insert without device id 
        try {
            phoneNumbersDaoWithConstraints.writeWithKey(PhonenumbersTable.NUMBER, "08834334")
                                          .value(PhonenumbersTable.ACTIVE, true)
                                          .ifNotExits()
                                          .execute();
            Assert.fail("ConstraintException expected");
        } catch (ConstraintException expected) {
            Assert.assertTrue(expected.getMessage().contains("columnn 'device_id' is mandatory"));
        }
        

        // insert with unknown device id 
        try {
            phoneNumbersDaoWithConstraints.writeWithKey(PhonenumbersTable.NUMBER, "08834334")
                                          .value(PhonenumbersTable.DEVICE_ID, "doesNotExits")
                                          .value(PhonenumbersTable.ACTIVE, true)
                                          .ifNotExits()
                                          .execute();
            Assert.fail("ConstraintException expected");
        } catch (ConstraintException expected) {
            Assert.assertTrue(expected.getMessage().contains("device with id"));
        }
        
        
      
        
        
        // read 
        phoneNumbersDaoWithConstraints.readWithKey(PhonenumbersTable.NUMBER, "0089645454455")
                                      .execute()
                                      .get();
        

        phoneNumbersDaoWithConstraints.readWithKey(PhonenumbersTable.NUMBER, "0089645454455")
                                      .column("active")
                                      .execute()
                                      .get();

        

        // modify record to make it inconsistent 
        phoneNumbersDao.writeWithKey(PhonenumbersTable.NUMBER, "0089645454455")
                       .value("device_id", "2333243")
                       .execute();

        
        // read inconsistent record
        try {
            phoneNumbersDaoWithConstraints.readWithKey(PhonenumbersTable.NUMBER, "0089645454455")
                                          .execute()
                                          .get();
            Assert.fail("ConstraintException expected");
        } catch (ConstraintException expected) {
            Assert.assertTrue(expected.getMessage().contains("reverse reference devices table -> phone_numbers table does not exit"));
        }
            

        try {
            phoneNumbersDaoWithConstraints.readWithKey(PhonenumbersTable.NUMBER, "0089645454455")
                                          .column("active")
                                          .execute()
                                          .get();
    
            Assert.fail("ConstraintException expected");
        } catch (ConstraintException expected) {
            Assert.assertTrue(expected.getMessage().contains("reverse reference devices table -> phone_numbers table does not exit"));
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

}



