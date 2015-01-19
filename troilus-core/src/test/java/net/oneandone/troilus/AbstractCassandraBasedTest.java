package net.oneandone.troilus;



import java.io.IOException;

import net.oneandone.troilus.api.FeesTable;
import net.oneandone.troilus.api.IdsTable;
import net.oneandone.troilus.api.LoginsTable;
import net.oneandone.troilus.api.PlusLoginsTable;
import net.oneandone.troilus.api.UsersTable;
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


