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

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.service.CassandraDaemon;
import org.yaml.snakeyaml.Yaml;

import com.google.common.base.Charsets;
import com.google.common.io.Closeables;



public class EmbeddedCassandra {

    private static final String CASSANDRA_YAML_FILE = "cassandra.yaml";

    private static CassandraDaemon cassandraDaemon;

    private static int nativePort = 0;
    
    
    
    public static void start() throws IOException {
        if (nativePort == 0) {
            nativePort = prepare();
    
            cassandraDaemon = new CassandraDaemon();
            cassandraDaemon.activate();
    
            
            long maxWaiting = System.currentTimeMillis() + 10000;
            
            while (System.currentTimeMillis() < maxWaiting) {
                if (cassandraDaemon.nativeServer.isRunning()) {
                    return;
                }
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) { }
            }
            throw new RuntimeException("Embedded cassandra does not start at expected time!");
        }
    }

    
    public static void close() {
        try {
            cassandraDaemon.thriftServer.stop();
            cassandraDaemon.nativeServer.stop();
        } catch (Throwable t) {
            t.printStackTrace();
        }
        
        nativePort = 0;
    }

    
    public static InetSocketAddress getNodeaddress() {
        return new InetSocketAddress("localhost", nativePort);
    }
    


    
    @SuppressWarnings("unchecked")
    protected static int prepare() throws IOException {
        
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

            cassandraConfigurationOutput =
              new OutputStreamWriter(new FileOutputStream(cassandraDirName + File.separator + CASSANDRA_YAML_FILE), Charsets.UTF_8);

            yaml.dump(cassandraConfiguration, cassandraConfigurationOutput);
            
            
            
            System.setProperty("cassandra.config", new File(cassandraDirName, CASSANDRA_YAML_FILE).toURI().toString());
            System.setProperty("cassandra-foreground", "true");
        
            DatabaseDescriptor.createAllDirectories();
            
            
            return nativeTransportPort;
            
        } finally {
            Closeables.closeQuietly(cassandraConfigurationInput);
            Closeables.close(cassandraConfigurationOutput, true);
        }
    }

    private static int findUnusedLocalPort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }
}
