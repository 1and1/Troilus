package net.oneandone.troilus.api;



import java.util.Map;
import java.util.Optional;

import net.oneandone.troilus.AbstractCassandraBasedTest;
import net.oneandone.troilus.Dao;
import net.oneandone.troilus.DaoImpl;
import net.oneandone.troilus.Record;

import org.junit.Assert;
import org.junit.Test;

import com.datastax.driver.core.ConsistencyLevel;
import com.google.common.collect.ImmutableMap;


public class WideRowTest extends AbstractCassandraBasedTest {
    
     
    
    @Test
    public void testIdsTable() throws Exception {
        Dao userDao = new DaoImpl(getSession(), IdsTable.TABLE)
                                .withConsistency(ConsistencyLevel.ONE);
        
        // insert
        userDao.writeWithKey(IdsTable.ID, "GLOBAL")
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


