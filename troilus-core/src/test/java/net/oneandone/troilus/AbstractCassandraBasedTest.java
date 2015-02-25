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



import java.io.File;
import java.io.IOException;
import java.util.Random;



import org.junit.AfterClass;
import org.junit.BeforeClass;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.Files;
import com.google.common.io.Resources;



public abstract class AbstractCassandraBasedTest {

    private static Cluster cluster;
    private final Session session;


    
    @BeforeClass
    public static void beforeClass() throws IOException {
        EmbeddedCassandra.start();
        
        cluster = Cluster.builder()
                         .addContactPointsWithPorts(ImmutableSet.of(EmbeddedCassandra.getNodeaddress()))
                         .build();
    }
    
    @AfterClass
    public static void afterClass() {
        cluster.close();
    }   
    
    
    public AbstractCassandraBasedTest() {
        String keyspacename = "ks_" + new Random().nextInt(999999999);
        createKeyspace(keyspacename);
        
        session = cluster.connect(keyspacename);
    }

    private void createKeyspace(String keyspacename) {
        try (Session session = cluster.connect("system")) {
            session.execute("create keyspace " + keyspacename + " WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };");
        }
    }
    

    
    
    protected Session getSession() {
        return  session;
    }

  
    protected void executeCqlFile(String cqlFile) throws IOException {
        File file = new File(cqlFile);
        if (file.exists()) {
            executeCql(Files.toString(new File(cqlFile), Charsets.UTF_8));
        } else {
            executeCql(Resources.toString(Resources.getResource(cqlFile), Charsets.UTF_8));
        }
    }
    
    protected void tryExecuteCqlFile(String cqlFile) {
        try {
            File file = new File(cqlFile);
            if (file.exists()) {
                tryExecuteCql(Files.toString(new File(cqlFile), Charsets.UTF_8));
            } else {
                tryExecuteCql(Resources.toString(Resources.getResource(cqlFile), Charsets.UTF_8));
            }
        } catch (IOException ioe) {
            throw new RuntimeException(cqlFile + " not found");
        }
    }

    protected void executeCql(String cql) {
        session.execute(cql);
    }
    
    protected void tryExecuteCql(String cql) {
        try {
            session.execute(cql);
        } catch (RuntimeException e) { 
            e.printStackTrace();
        }
    }
}


