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



import net.oneandone.troilus.AbstractCassandraBasedTest;
import net.oneandone.troilus.Dao;
import net.oneandone.troilus.DaoImpl;

import org.junit.Test;

import com.datastax.driver.core.ConsistencyLevel;



public class HistroyTableTest extends AbstractCassandraBasedTest {
    
     
    
    @Test
    public void testHistoryTable() throws Exception {
        Dao history = new DaoImpl(getSession(), HistoryTable.TABLE)
                                .withConsistency(ConsistencyLevel.ONE);
        
        // insert
        history.writeWithKey(HistoryTable.SENDER_EMAIL, "sender@example.org", HistoryTable.RECEIVER_EMAIL, "receiver@example.org")
               .execute();
    }        
}


