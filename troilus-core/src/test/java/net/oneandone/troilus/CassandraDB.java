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
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.UUID;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.service.CassandraDaemon;
import org.yaml.snakeyaml.Yaml;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.google.common.base.Charsets;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.io.Closeables;
import com.google.common.io.Files;
import com.google.common.io.Resources;


/**
 * 
 * @author Jason Westra - edited original
 * 12-12-2015: 3.x API change
 *
 */
public class CassandraDB {

    private static final String CASSANDRA_YAML_FILE = "cassandra.yaml";

    private static CassandraDaemon cassandraDaemon;
    private static int nativePort = 0;
    private static Cluster globalCluster;
    private static Session globalSession;
    private static String globalKeyspacename;
  
    private static final Set<String> running = Sets.newCopyOnWriteArraySet();
    
    
    private final String id;
    private final int port;
    private final Cluster cluster;
    private final Session session;
    
    
    public CassandraDB(int port) {
        this.port = port;
        this.id = UUID.randomUUID().toString();
        
        running.add(id);
        

        this.cluster = globalCluster;
        this.session = globalSession;
    }

        
 
    
    public void close() {
      
        new Thread() {
                
            public void run() {
                running.remove(id);
                
                try {
                    Thread.sleep(1 * 60 * 1000);
                } catch (InterruptedException ignore) { }

                if (running.isEmpty()) {
                    shutdown();
                }
            };
                
        }.start();
    }
    
    
    public Session getSession() {
        return session;
    }
        
    public Session newSession() {
        Cluster cluster = createCluster();
        return createSession(cluster);
    }

    
    public Session newGobalSession() {
        Cluster cluster = createCluster();
        return cluster.connect();
    }

    
    public Session newSession(String keyspacename) {
        Cluster cluster = createCluster();
        return cluster.connect(keyspacename);
    }
    
    
    
    
    public String getKeyspacename() {
        return globalKeyspacename;
    }
    
    /**
     * executes a CQL file 
     * 
     * @param cqlFile    the CQL file name 
     * @throws IOException  if the file could not be found
     */
    public void executeCqlFile(String cqlFile) throws IOException {
        File file = new File(cqlFile);
        if (file.exists()) {
            executeCql(Files.toString(new File(cqlFile), Charsets.UTF_8));
        } else {
            executeCql(Resources.toString(Resources.getResource(cqlFile), Charsets.UTF_8));
        }
    }
    
    
    /**
     * executes a CQL file. CQL processing errors will be ignored 
     * 
     * @param cqlFile    the CQL file name 
     * @throws IOException  if the file could not be found
     */
    public void tryExecuteCqlFile(String cqlFile) {
        try {
            File file = new File(cqlFile);
            if (file.exists()) {
                tryExecuteCqls(Splitter.on(";").split(Files.toString(new File(cqlFile), Charsets.UTF_8)));
            } else {
                tryExecuteCqls(Splitter.on(";").split(Resources.toString(Resources.getResource(cqlFile), Charsets.UTF_8)));
            }
        } catch (IOException ioe) {
            throw new RuntimeException(cqlFile + " not found");
        }
    }

    
    /**
     * executes CQL 
     * 
     * @param cql    the CQL to execute  
     */
    public void executeCql(String cql) {
        session.execute(cql);
    }
 
    
    /**
     * executes CQL. CQL processing errors will be ignored 
     * 
     * @param cql  the CQL to execute
     */
    public void tryExecuteCql(String cql) {
        try {
            session.execute(cql);
        } catch (RuntimeException e) { 
            e.printStackTrace();
        }
    }
 
    
    public void tryExecuteCqls(Iterable<String> cqls) {
        for (String cql : cqls) {
            tryExecuteCql(cql);
        }
    }
    
    public static CassandraDB newInstance() {
        start();
        return new CassandraDB(nativePort);
    }
    
    
    
    
    
    private static void start() {
        
        if (nativePort == 0) {
            nativePort = prepare();
    
            cassandraDaemon = new CassandraDaemon();
            cassandraDaemon.activate();
    
            
            long maxWaiting = System.currentTimeMillis() + 10000;
            
            while (System.currentTimeMillis() < maxWaiting) {
            	// jwestra: 3.x API change
            	if (cassandraDaemon.isNativeTransportRunning()) {
                //if (cassandraDaemon.nativeServer.isRunning()) {
                    init();
                    return;
                }
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) { }
            }
            throw new RuntimeException("Embedded cassandra does not start at expected time!");
            
            
        }
    }
    
    private static void init() {
        globalKeyspacename = "ks_" + UUID.randomUUID().toString().replace("-", "");
        
        globalCluster = createCluster();
        createKeyspace(globalCluster, globalKeyspacename);

        globalSession = createSession(globalCluster);
    }
    
    
    private static void createKeyspace(Cluster cluster, String keyspacename) {
        try (Session session = cluster.connect("system")) {
            session.execute("create keyspace " + keyspacename + " WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };");
        }
    }
    
    
    public static Cluster createCluster() {
        return Cluster.builder()
                      .addContactPointsWithPorts(ImmutableSet.of(new InetSocketAddress("localhost", nativePort)))
                      .build();
    }
    
    private static Session createSession(Cluster cluster) {
        return cluster.connect(globalKeyspacename);
    }


    
    private static void shutdown() {
        try {
            cassandraDaemon.thriftServer.stop();
            
            // jwestra: 3.x API change
            cassandraDaemon.stop();
            //cassandraDaemon.nativeServer.stop();
        } catch (Throwable t) {
            t.printStackTrace();
        }
        
        nativePort = 0;
    }


    
    @SuppressWarnings("unchecked")
    protected static int prepare() {
        String cassandraDirName = "target" + File.separator + "cassandra-junit-" + new Random().nextInt(1000000);
        
        
        File cassandraDir = new File(cassandraDirName);
        cassandraDir.mkdirs();
        

        InputStream cassandraConfigurationInput = null;
        Writer cassandraConfigurationOutput = null;

        try {
            ClassLoader loader = Thread.currentThread().getContextClassLoader();
            cassandraConfigurationInput = loader.getResourceAsStream(CASSANDRA_YAML_FILE);

            Yaml yaml = new Yaml();
            Map<String, Object> cassandraConfiguration = (Map<String, Object>)yaml.load(cassandraConfigurationInput);

            int rpcPort = findUnusedLocalPort();
            if (rpcPort == -1) {
                throw new RuntimeException("Can not start embedded cassandra: no unused local port found!");
            }
            cassandraConfiguration.put("rpc_port", rpcPort);

            int storagePort = findUnusedLocalPort();
            if (storagePort == -1) {
                throw new RuntimeException("Can not start embedded cassandra: no unused local port found!");
            }
            cassandraConfiguration.put("storage_port", storagePort);

            int nativeTransportPort = findUnusedLocalPort();
            if (nativeTransportPort == -1) {
                throw new RuntimeException("Can not start embedded cassandra: no unused local port found!");
            }
            cassandraConfiguration.put("native_transport_port", nativeTransportPort);

            cassandraConfiguration.put("start_native_transport", "true");

            Object obj = cassandraConfiguration.get("data_file_directories");
            
            cassandraConfiguration.put("data_file_directories", Lists.newArrayList(new File(cassandraDir, "data").getAbsolutePath()));
            cassandraConfiguration.put("commitlog_directory", new File(cassandraDir, "commitlog").getAbsolutePath());
            cassandraConfiguration.put("saved_caches_directory", new File(cassandraDir, "saved_caches").getAbsolutePath());
            
            // jwestra: 3.x API change: resolve error on start of 'hints_directory' is missing
            cassandraConfiguration.put("hints_directory", new File(cassandraDir, "hints_directory").getAbsolutePath());
                      
            cassandraConfigurationOutput =
              new OutputStreamWriter(new FileOutputStream(cassandraDirName + File.separator + CASSANDRA_YAML_FILE), Charsets.UTF_8);

            // jwestra: 3.x API change: resolve error on start of -Dcassandra.storage_dir is not set
            String storageDir = new File(cassandraDir, "storageDir").getAbsolutePath();
            System.setProperty("cassandra.storage_dir", storageDir);
            // end: 3.x API change
            
            yaml.dump(cassandraConfiguration, cassandraConfigurationOutput);
            
            
            System.setProperty("cassandra.config", new File(cassandraDirName, CASSANDRA_YAML_FILE).toURI().toString());
            System.setProperty("cassandra-foreground", "true");
        
            DatabaseDescriptor.createAllDirectories();
            
            
            return nativeTransportPort;
            
        } catch (IOException ioe) {
            throw new RuntimeException(ioe); 
            
        } finally {
            Closeables.closeQuietly(cassandraConfigurationInput);
            try {
                Closeables.close(cassandraConfigurationOutput, true);
            } catch (IOException ignore) {

            }
        }
    }

    private static int findUnusedLocalPort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }
}