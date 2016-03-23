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


import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.exceptions.DriverInternalError;
import com.datastax.driver.core.exceptions.InvalidQueryException;
import com.datastax.driver.core.querybuilder.BuiltStatement;
import com.google.common.base.Joiner;
import com.google.common.base.MoreObjects;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;



/**
 * DBSession
 */
class DBSession  {
    private static final Logger LOG = LoggerFactory.getLogger(DBSession.class);
    private final Session session;
    private final boolean isKeyspacenameAssigned;
    private final String keyspacename;
    private final PreparedStatementCache preparedStatementCache;
    private final AtomicLong lastCacheCleanTime = new AtomicLong(0);

    /**
     * constructor 
     * @param session    the underlying session
     * @param catalog    the metadata catalog
     * @param beanMapper the bean mapper
     */
    DBSession(final Session session, final MetadataCatalog catalog, final BeanMapper beanMapper) {
        this.session = session;
        this.keyspacename = session.getLoggedKeyspace();
        this.isKeyspacenameAssigned = (keyspacename != null);
        this.preparedStatementCache = new PreparedStatementCache(session);
    }


  
    /**
     * @return true, if a keyspacename is assigned to this context
     */
    boolean isKeyspaqcenameAssigned() {
        return isKeyspacenameAssigned; 
    }
    
    /**
     * @return the keyspacename or null
     */
    String getKeyspacename() {
        return keyspacename;
    }
    
    private Session getSession() {
        return session;
    }
    
    /**
     * @return the protocol version
     */
    ProtocolVersion getProtocolVersion() {
        return getSession().getCluster().getConfiguration().getProtocolOptions().getProtocolVersion();
    }
 
    /**
     * @param statement the statement to prepare
     * @return the prepared statement future
     */
    CompletableFuture<PreparedStatement> prepareAsync(final BuiltStatement statement) {
        return preparedStatementCache.prepareAsync(statement);
    }
    
    /**
     * @param statement  the statement to execute in an async manner
     * @return the resultset future
     */
    public CompletableFuture<ResultSet> executeAsync(final Statement statement) {
        try {
            LOG.debug("executing " + statement);
            return CompletableFutures.toCompletableFuture(getSession().executeAsync(statement))
                                     .thenApply(resultSet -> {
                                                                 LOG.debug("got " + resultSet);
                                                                 return resultSet;
                                                             });
        } catch (InvalidQueryException | DriverInternalError e) {
            cleanUp();
            LOG.warn("could not execute statement", e);
            return CompletableFutures.failedFuture(e);
        }
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                          .add("preparedStatementsCache", preparedStatementCache.toString())
                          .toString();
    }
  
    
    private void cleanUp() {
        // avoid bulk clean calls within the same time
        if (System.currentTimeMillis() > (lastCacheCleanTime.get() + 1600)) {  // not really thread safe. However this does not matter 
            lastCacheCleanTime.set(System.currentTimeMillis());
            preparedStatementCache.invalidateAll();
        }
    }
    
    
    private static final class PreparedStatementCache {
        private final Session session;
        private final Cache<String, PreparedStatement> preparedStatementCache;

        public PreparedStatementCache(Session session) {
            this.session = session;
            this.preparedStatementCache = CacheBuilder.newBuilder().maximumSize(150).<String, PreparedStatement>build();
        }
        
        CompletableFuture<PreparedStatement> prepareAsync(final BuiltStatement statement) {
            final PreparedStatement preparedStatment = preparedStatementCache.getIfPresent(statement.getQueryString());
            if (preparedStatment == null) {
                LOG.debug("statement " + statement + " not found in prepared statement cache. perparing it");
                return CompletableFutures.toCompletableFuture(session.prepareAsync(statement))
                                         .thenApply(stmt -> {
                                                             preparedStatementCache.put(statement.getQueryString(), stmt);
                                                             LOG.debug("statement " + statement + " prepared and added to cache");
                                                             return stmt;
                                                            });
            } else {
                return CompletableFuture.completedFuture(preparedStatment);
            }
        }
        
        public void invalidateAll() {
            preparedStatementCache.invalidateAll();
        }      
        
        @Override
        public String toString() {
            return Joiner.on(", ").withKeyValueSeparator("=").join(preparedStatementCache.asMap());
        }
    }
}