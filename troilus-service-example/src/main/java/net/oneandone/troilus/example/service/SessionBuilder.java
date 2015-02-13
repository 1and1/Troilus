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
package net.oneandone.troilus.example.service;

import java.net.InetSocketAddress;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.google.common.collect.ImmutableSet;




public class SessionBuilder {
    private final String keyspace;
    private final String host;
    private final int port;

    public SessionBuilder(String keyspace, String host, int port) {
        this.keyspace = keyspace;
        this.host = host;
        this.port = port;
    }
    
    public String getKeyspace() {
        return keyspace;
    }

    public String getHost() {
        return host;
    }
    
    public int getPort() {
        return port;
    }
    
    public Session build() {
        return Cluster.builder()
                     .addContactPointsWithPorts(ImmutableSet.of(new InetSocketAddress(host, port)))
                     .build()
                     .connect(keyspace);
    }
}
