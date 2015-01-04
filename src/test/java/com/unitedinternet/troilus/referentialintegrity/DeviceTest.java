package com.unitedinternet.troilus.referentialintegrity;


import org.junit.Test;







import com.datastax.driver.core.ConsistencyLevel;
import com.unitedinternet.troilus.AbstractCassandraBasedTest;
import com.unitedinternet.troilus.Dao;
import com.unitedinternet.troilus.DaoImpl;



public class DeviceTest extends AbstractCassandraBasedTest {
    
    
    @Test
    public void testExample() throws Exception {
                
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


