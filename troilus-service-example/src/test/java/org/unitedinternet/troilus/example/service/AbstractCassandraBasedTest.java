package org.unitedinternet.troilus.example.service;



import java.io.IOException;

import org.junit.AfterClass;
import org.junit.BeforeClass;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.google.common.collect.ImmutableSet;



public abstract class AbstractCassandraBasedTest {
    
    private static final String KEYYSPACENAME = "testks1";
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
        session.execute(HotelsTable.CREATE_STMT);
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


