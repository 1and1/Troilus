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
package net.oneandone.troilus;



import java.io.IOException;

import net.oneandone.troilus.api.FeesTable;
import net.oneandone.troilus.api.HistoryTable;
import net.oneandone.troilus.api.IdsTable;
import net.oneandone.troilus.api.LoginsTable;
import net.oneandone.troilus.api.PlusLoginsTable;
import net.oneandone.troilus.api.UsersTable;
import net.oneandone.troilus.cascade.KeyByAccountColumns;
import net.oneandone.troilus.cascade.KeyByEmailColumns;
import net.oneandone.troilus.example.AddressType;
import net.oneandone.troilus.example.HotelsTable;
import net.oneandone.troilus.example.RoomsTable;
import net.oneandone.troilus.referentialintegrity.DeviceTable;
import net.oneandone.troilus.referentialintegrity.PhonenumbersTable;
import net.oneandone.troilus.userdefinieddatatypes.AddrType;
import net.oneandone.troilus.userdefinieddatatypes.AddresslineType;
import net.oneandone.troilus.userdefinieddatatypes.ClassifierType;
import net.oneandone.troilus.userdefinieddatatypes.CustomersTable;
import net.oneandone.troilus.userdefinieddatatypes.ScoreType;

import org.junit.AfterClass;
import org.junit.BeforeClass;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.google.common.collect.ImmutableSet;



public abstract class AbstractCassandraBasedTest {
    
    private static final String KEYYSPACENAME = "testks";
    private static Cluster cluster;
    private static Session session;
    
    
    
    @BeforeClass
    public static void setup() throws IOException {
        EmbeddedCassandra.start();
        
        cluster = Cluster.builder()
                         .addContactPointsWithPorts(ImmutableSet.of(EmbeddedCassandra.getNodeaddress()))
                         .build();
        
        
        
        dropKeyspace(cluster);
        createKeyspace(cluster);
        
        session = cluster.connect(KEYYSPACENAME);
        createTables(session);
    }

    

    private static final void dropKeyspace(Cluster cluster) {
        try (Session session = cluster.connect("system")) {
            session.execute("drop keyspace " + KEYYSPACENAME);
        } catch (RuntimeException e) {
            e.printStackTrace();
        }
    }
    
    
    private static void createKeyspace(Cluster cluster) {
        try (Session session = cluster.connect("system")) {
            session.execute("create keyspace " + KEYYSPACENAME + " WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };");
        }
    }
    
    
    private static void createTables(Session session)  {
        session.execute(ClassifierType.CREATE_STMT);
        session.execute(ScoreType.CREATE_STMT);
        session.execute(AddresslineType.CREATE_STMT);
        session.execute(AddrType.CREATE_STMT);
        session.execute(UsersTable.CREATE_STMT);
        session.execute(LoginsTable.CREATE_STMT);
        session.execute(PlusLoginsTable.CREATE_STMT);
        session.execute(FeesTable.CREATE_STMT);
        session.execute(IdsTable.CREATE_STMT);
        session.execute(AddressType.CREATE_STMT);
        session.execute(HotelsTable.CREATE_STMT);
        session.execute(RoomsTable.CREATE_STMT);
        session.execute(CustomersTable.CREATE_STMT);
        session.execute(DeviceTable.CREATE_STMT);
        session.execute(PhonenumbersTable.CREATE_STMT);
        session.execute(HistoryTable.CREATE_STMT);
        session.execute(KeyByAccountColumns.CREATE_STMT);
        session.execute(KeyByEmailColumns.CREATE_STMT);
    }
    
    
    @AfterClass
    public static void teardown() {
        session.close();
        cluster.close();
    }    
    
    
    protected static Session getSession() {
        return  session;
    }
}


