package com.unitedinternet.troilus.referentialintegrity;




import java.util.Optional;

import org.junit.Assert;
import org.junit.Test;











import com.datastax.driver.core.ConsistencyLevel;
import com.google.common.collect.ImmutableMap;
import com.unitedinternet.troilus.AbstractCassandraBasedTest;
import com.unitedinternet.troilus.Dao;
import com.unitedinternet.troilus.DaoImpl;
import com.unitedinternet.troilus.InsertQueryPreInterceptor;



public class DeviceTest extends AbstractCassandraBasedTest {
    
    

    @Test
    public void testUnmodifyableColumn() throws Exception {
                
        Dao phoneToDeviceDao = new DaoImpl(getSession(), PhoneToDeviceTable.TABLE)
                                   .withConsistency(ConsistencyLevel.LOCAL_QUORUM)
                                   .withSerialConsistency(ConsistencyLevel.SERIAL);


        phoneToDeviceDao.writeWithKey(PhoneToDeviceTable.NUMBER, "0089645454455")
                        .value(PhoneToDeviceTable.DEVICE_ID, "deaeea")
                        .value(PhoneToDeviceTable.ACTIVE, true)
                        .ifNotExits()
                        .execute();
        
        
        phoneToDeviceDao.writeWithKey(PhoneToDeviceTable.NUMBER, "0089123234234")
                        .value(PhoneToDeviceTable.DEVICE_ID, "dfacbsd")
                        .value(PhoneToDeviceTable.ACTIVE, true)
                        .ifNotExits()
                        .execute();
        
        
        // with unmodifiyable constraint
        Dao phoneToDeviceDaoWithConstraint = phoneToDeviceDao.withInterceptor(new UnmodifyableColumnInterceptor());
        
        // update modifyable column
        phoneToDeviceDaoWithConstraint.writeWithKey(PhoneToDeviceTable.NUMBER, "0089123234234")
                                      .value(PhoneToDeviceTable.ACTIVE, false)
                                      .execute();


        // update non-modifyable column
  /*      try {
            phoneToDeviceDaoWithConstraint.writeWithKey(PhoneToDeviceTable.NUMBER, "0089123234234")
                                          .value(PhoneToDeviceTable.DEVICE_ID, "dfacbsd")
                                          .execute();
            Assert.fail("RuntimeException expected");
        } catch (RuntimeException expected) {  }*/
    }
    

    
    private static final class UnmodifyableColumnInterceptor implements InsertQueryPreInterceptor {
        
        @Override
        public void onPreInsert( ImmutableMap<String, Optional<Object>> valuesToMutate, boolean ifNotExists) {
            
        }
    }
    
    

    @Test
    public void testSimple() throws Exception {
                
        // create dao
        Dao deviceDao = new DaoImpl(getSession(), DeviceTable.TABLE)
                                  .withConsistency(ConsistencyLevel.LOCAL_QUORUM)
                                  .withSerialConsistency(ConsistencyLevel.SERIAL);

        Dao phoneToDeviceDao = new DaoImpl(getSession(), PhoneToDeviceTable.TABLE)
                                   .withConsistency(ConsistencyLevel.LOCAL_QUORUM)
                                   .withSerialConsistency(ConsistencyLevel.SERIAL);


        
        deviceDao.writeWithKey(DeviceTable.ID, "834343")
                 .value(DeviceTable.TYPE, 3)
                 .ifNotExits()
                 .execute();
        
        
        deviceDao.writeWithKey(DeviceTable.ID, "2333243")
                 .value(DeviceTable.TYPE, 1)
                 .ifNotExits()
                 .execute();

        
        deviceDao.writeWithKey(DeviceTable.ID, "934453434")
                 .value(DeviceTable.TYPE, 3)
                 .ifNotExits()
                 .execute();
    }        
}


