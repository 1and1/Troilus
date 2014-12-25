package com.unitedinternet.troilus;



import java.io.IOException;

import org.junit.AfterClass;
import org.junit.BeforeClass;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.google.common.collect.ImmutableSet;
import com.unitedinternet.troilus.api.FeesTable;
import com.unitedinternet.troilus.api.IdsTable;
import com.unitedinternet.troilus.api.UsersTable;
import com.unitedinternet.troilus.example.HotelsTable;
import com.unitedinternet.troilus.example.RoomsTable;
import com.unitedinternet.troilus.userdefinieddatatypes.AddressType;
import com.unitedinternet.troilus.userdefinieddatatypes.AddresslineType;
import com.unitedinternet.troilus.userdefinieddatatypes.ClassifierType;
import com.unitedinternet.troilus.userdefinieddatatypes.CustomersTable;
import com.unitedinternet.troilus.userdefinieddatatypes.ScoreType;



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
        session.execute(AddressType.CREATE_STMT);
        session.execute(UsersTable.CREATE_STMT);
        session.execute(FeesTable.CREATE_STMT);
        session.execute(IdsTable.CREATE_STMT);
        session.execute(HotelsTable.CREATE_STMT);
        session.execute(RoomsTable.CREATE_STMT);
        session.execute(CustomersTable.CREATE_STMT);
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


