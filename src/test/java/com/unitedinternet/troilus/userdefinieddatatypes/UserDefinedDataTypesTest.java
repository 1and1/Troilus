package com.unitedinternet.troilus.userdefinieddatatypes;




import org.junit.Assert;
import org.junit.Test;

import com.datastax.driver.core.ConsistencyLevel;
import com.unitedinternet.troilus.AbstractCassandraBasedTest;
import com.unitedinternet.troilus.Dao;
import com.unitedinternet.troilus.DaoManager;
import com.unitedinternet.troilus.Record;



public class UserDefinedDataTypesTest extends AbstractCassandraBasedTest {
    

        
    
    @Test
    public void testSimpleTable() throws Exception {
        DaoManager daoManager = new DaoManager(getSession());

        Dao customersDao = daoManager.getDao(CustomersTable.TABLE)
                                     .withConsistency(ConsistencyLevel.LOCAL_QUORUM);

        

/*
        
        ////////////////
        // inserts
        customersDao.writeWithKey(CustomersTable.ID, "95453543534")
                    .value("name", "peter")
                    .value("address", "street", "brauerstrasse")
                    .value("address", "zip_code", "76244")
                    .execute();

        
        Record record = customersDao.readWithKey(CustomersTable.ID, "95453543534")
                                    .execute()
                                    .get();

        Assert.assertEquals("brauerstrasse", record.getString("address", "street").get());
        Assert.assertEquals("76244", record.getString("address", "zip_code").get());
    */    
    }               
}


