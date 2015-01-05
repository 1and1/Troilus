package com.unitedinternet.troilus.userdefinieddatatypes;




import org.junit.Assert;
import org.junit.Test;

import com.datastax.driver.core.ConsistencyLevel;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.unitedinternet.troilus.AbstractCassandraBasedTest;
import com.unitedinternet.troilus.Dao;
import com.unitedinternet.troilus.DaoImpl;
import com.unitedinternet.troilus.Record;



public class UserDefinedDataTypesTest extends AbstractCassandraBasedTest {
    

        
    
    @Test
    public void testSimpleTable() throws Exception {
        Dao customersDao = new DaoImpl(getSession(), CustomersTable.TABLE)
                                     .withConsistency(ConsistencyLevel.LOCAL_QUORUM);

        

        //////////////// 
        // inserts
        customersDao.writeWithKey(CustomersTable.ID, "95453543534")
                    .value(CustomersTable.NAME, "peter")
                    .value(CustomersTable.ROLES, ImmutableMap.of("user", "rtrtr", "employee", "e33"))
                    .value(CustomersTable.PHONE_NUMBERS, ImmutableSet.of("454545", "2354234324"))
                    .value(CustomersTable.CURRENT_ADDRESS, new Addr(ImmutableList.of(new Addressline("brauerstrasse")), 76336, ImmutableMap.of("a1", new Addressline("alias1"))))
                    .value(CustomersTable.OLD_ADDRESSES, ImmutableSet.of(new Addr(ImmutableList.of(new Addressline("frankfurter ring")), 80445, ImmutableMap.of("a2", new Addressline("alias2")))))
                    .value(CustomersTable.CLASSIFICATION, ImmutableMap.of(new Classifier("reliability"), new Score(23)))
                    .value(CustomersTable.CLASSIFICATION2, ImmutableMap.of(5, new Score(23)))
                    .execute();

        
        Record record = customersDao.readWithKey(CustomersTable.ID, "95453543534")
                                    .execute()
                                    .get();
        
        Assert.assertEquals("peter", record.getString(CustomersTable.NAME).get());
        Assert.assertTrue(record.getSet(CustomersTable.PHONE_NUMBERS, String.class).get().contains("454545"));       
        Assert.assertEquals("e33", record.getMap(CustomersTable.ROLES, String .class, String.class).get().get("employee"));
        Assert.assertEquals("brauerstrasse", record.getObject(CustomersTable.CURRENT_ADDRESS, Addr.class).get().getLines().get(0).getLine());
        Assert.assertEquals("frankfurter ring", record.getSet(CustomersTable.OLD_ADDRESSES, Addr.class).get().iterator().next().getLines().get(0).getLine());
        Assert.assertEquals((Integer) 23, record.getMap(CustomersTable.CLASSIFICATION, Classifier.class, Score.class).get().get(new Classifier("reliability")).getScore());
        Assert.assertEquals((Integer) 23, record.getMap(CustomersTable.CLASSIFICATION2, Integer.class, Score.class).get().get(5).getScore());
    }               
}


