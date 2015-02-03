/*
 * Copyright 1&1 Internet AG, https://github.com/1and1/
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
        
        Map<String, Integer> keys = (Map<String, Integer>) optionalRecord.get().getMap(IdsTable.IDS, String.class, Integer.class);
        Assert.assertEquals(23, (int) keys.get("ID_433433"));        
        Assert.assertEquals(556,(int) keys.get("ID_33434443"));
    }        
}


