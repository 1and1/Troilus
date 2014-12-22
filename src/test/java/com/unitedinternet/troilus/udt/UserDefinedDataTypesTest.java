package com.unitedinternet.troilus.udt;



import org.junit.Test;

import com.datastax.driver.core.ConsistencyLevel;
import com.unitedinternet.troilus.AbstractCassandraBasedTest;
import com.unitedinternet.troilus.Dao;
import com.unitedinternet.troilus.DaoManager;



public class UserDefinedDataTypesTest extends AbstractCassandraBasedTest {
    

        
    
    @Test
    public void testSimpleTable() throws Exception {
        DaoManager daoManager = new DaoManager(getSession());

        Dao customersDao = daoManager.getDao(CustomersTable.TABLE)
                                     .withConsistency(ConsistencyLevel.LOCAL_QUORUM);

        


        
        ////////////////
        // inserts
        customersDao.writeWithKey(CustomersTable.ID, "95453543534")
                    .value("name", "peter")
      //              .value("address", "street", "brauerstrasse")
                    .execute();
        

        
    }               
}


