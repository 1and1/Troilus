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
package net.oneandone.troilus.cascade;



import java.util.Optional;



import net.oneandone.troilus.AbstractCassandraBasedTest;
import net.oneandone.troilus.Dao;
import net.oneandone.troilus.Record;

import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import com.datastax.driver.core.ConsistencyLevel;
import com.google.common.collect.ImmutableMap;



public class CascadingTest extends AbstractCassandraBasedTest {
    

    @Test
    public void testCasscading() throws Exception {   
        
        DaoManager daoManager = new DaoManager(getSession());
       
        Dao keyByAccountDao = daoManager.getKeyByAccountDao();
        Dao keyByEmailDao = daoManager.getKeyByEmailDao();

        
        String id = "act3344";
        byte[] key = new byte[] { 34, 56, 87, 88 };
        String email = "me@example.org";
        long time = System.currentTimeMillis(); 
        
        
        //////////////////////////////////////
        // insert 
        keyByAccountDao.writeWithKey(KeyByAccountColumns.ACCOUNT_ID, id)
                       .value(KeyByAccountColumns.KEY, key)
                       .value(KeyByAccountColumns.EMAIL_IDX, ImmutableMap.of(email, time))
                       .withConsistency(ConsistencyLevel.QUORUM)
                       .execute();
        
        
        
        // test 
        Record record = keyByEmailDao.readWithKey(KeyByEmailColumns.EMAIL, email, KeyByEmailColumns.CREATED, time)
                                     .withConsistency(ConsistencyLevel.QUORUM)
                                     .execute()
                                     .get();
        Assert.assertEquals(id, record.getValue(KeyByEmailColumns.ACCOUNT_ID));
        Assert.assertArrayEquals(key, record.getValue(KeyByEmailColumns.KEY));
        
        record = keyByAccountDao.readWithKey(KeyByAccountColumns.ACCOUNT_ID, id)
                                .withConsistency(ConsistencyLevel.QUORUM)
                                .execute()
                                .get();
        Assert.assertEquals(id, record.getValue(KeyByAccountColumns.ACCOUNT_ID));
        Assert.assertEquals((Long) time, record.getValue(KeyByAccountColumns.EMAIL_IDX).get(email));
        
        
        
        
        
        
        
        ///////////////////////////////////////////////////////
        // Delete
        
        keyByAccountDao.deleteWithKey(KeyByAccountColumns.ACCOUNT_ID, id)
                       .withConsistency(ConsistencyLevel.QUORUM)
                       .execute();
        
        
        
        Assert.assertEquals(Optional.empty(), keyByEmailDao.readWithKey(KeyByEmailColumns.EMAIL, email, KeyByEmailColumns.CREATED, time)
                                                           .withConsistency(ConsistencyLevel.QUORUM)
                                                           .execute());

        Assert.assertEquals(Optional.empty(), keyByAccountDao.readWithKey(KeyByAccountColumns.ACCOUNT_ID, id)
                                                             .withConsistency(ConsistencyLevel.QUORUM)
                                                             .execute());        
    }
}