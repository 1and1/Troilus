package com.unitedinternet.troilus.userdefinieddatatypes;




import org.junit.Assert;
import org.junit.Test;

import com.datastax.driver.core.ConsistencyLevel;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
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

        

        //////////////// 
        // inserts
        customersDao.writeWithKey(CustomersTable.ID, "95453543534")
                    .value(CustomersTable.NAME, "peter")
                    .value(CustomersTable.PHONE_NUMBERS, ImmutableSet.of("454545", "2354234324"))
                    .value(CustomersTable.CURRENT_ADDRESS, new Address(ImmutableList.of(new Addressline("brauerstrasse")), 76336))
                    .value(CustomersTable.OLD_ADDRESSES, ImmutableSet.of(new Address(ImmutableList.of(new Addressline("frankfurter ring")), 80445)))
                    .value(CustomersTable.CLASSIFICATION, ImmutableMap.of(new Classifier("reliability"), new Score(23)))
                    .value(CustomersTable.CLASSIFICATION2, ImmutableMap.of(5, new Score(23)))
                    .execute();

        
        Record record = customersDao.readWithKey(CustomersTable.ID, "95453543534")
                                    .execute()
                                    .get();
        
        Assert.assertEquals("peter", record.getString(CustomersTable.NAME).get());
        Assert.assertTrue(record.getSet(CustomersTable.PHONE_NUMBERS, String.class).get().contains("454545"));       
        Assert.assertEquals("brauerstrasse", record.getUDT(CustomersTable.CURRENT_ADDRESS, Address.class).get().getLines().get(0).getLine());
        Assert.assertEquals("frankfurter ring", record.getSet(CustomersTable.OLD_ADDRESSES, Address.class).get().iterator().next().getLines().get(0).getLine());
        
  
    }               
}


