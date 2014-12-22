package com.unitedinternet.troilus.api;



import java.util.Map;
import java.util.Optional;

import org.junit.Assert;
import org.junit.Test;

import com.datastax.driver.core.ConsistencyLevel;
import com.google.common.collect.ImmutableMap;
import com.unitedinternet.troilus.AbstractCassandraBasedTest;
import com.unitedinternet.troilus.Dao;
import com.unitedinternet.troilus.DaoManager;
import com.unitedinternet.troilus.Record;


public class WideRowTest extends AbstractCassandraBasedTest {
    
     
    
    @Test
    public void testIdsTable() throws Exception {
        DaoManager daoManager = new DaoManager(getSession());

        Dao userDao = daoManager.getDao(IdsTable.TABLE)
                                .withConsistency(ConsistencyLevel.ONE);
        
        // insert
        userDao.write()
               .value(IdsTable.ID, "GLOBAL")
               .value(IdsTable.IDS, ImmutableMap.of("ID_433433", 23, "ID_33434443", 556))
               .execute();
        
        
        // update
        // TODO
        
        // read single
        Optional<Record> optionalRecord = userDao.readWithKey(IdsTable.ID, "GLOBAL")
                                                 .column(IdsTable.IDS)
                                                 .execute();
        Assert.assertTrue(optionalRecord.isPresent());
        
        Map<String, Integer> keys = (Map<String, Integer>) optionalRecord.get().getMap(IdsTable.IDS, String.class, Integer.class).get();
        Assert.assertEquals(23, (int) keys.get("ID_433433"));        
        Assert.assertEquals(556,(int) keys.get("ID_33434443"));
    }        
}


