package org.unitedinternet.troilus.example.service;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.google.common.collect.ImmutableSet;




public class SessionBuilder {

    public Session build() {
        return Cluster.builder()
                     .addContactPointsWithPorts(ImmutableSet.of(EmbeddedCassandra.getNodeaddress()))
                     .build()
                     .connect("testks1");
    }
}
