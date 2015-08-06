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


import java.util.Set;



import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.TableMetadata;
import com.datastax.driver.core.UserType;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;



/**
 * MetadataCatalog including database metadata 
 * 
 * @author grro
 *
 */
class MetadataCatalog  {

    private final TableMetadataCache tableMetadataCache;
    private final UserTypeCache userTypeCache;

    /**
     * constructor
     * @param session  the underlying session
     */
    MetadataCatalog(Session session) {
        this.tableMetadataCache = new TableMetadataCache(session);
        this.userTypeCache = new UserTypeCache(session);
    }
    
    /**
     * @param tablename the tablename
     * @return the columnnames of this table
     */
    public ImmutableSet<String> getColumnNames(Tablename tablename) {
        return tableMetadataCache.getColumnNames(tablename);
    }
    
    /**
     * @param tablename  the tablename
     * @param columnName the cloumnname
     * @return the column metadata
     */
    public ColumnMetadata getColumnMetadata(Tablename tablename, String columnName) {
        return tableMetadataCache.getColumnMetadata(tablename, columnName); 
    }
    
    /**
     * @param tablename    the tablename
     * @param usertypeName the usertype name
     * @return the usertype
     */
    public UserType getUserType(Tablename tablename, String usertypeName) {
        return userTypeCache.get(tablename, usertypeName);
    }
    
    
    

    private static final class TableMetadataCache {
        private final Session session;
        private final Cache<Tablename, Metadata> tableMetadataCache;
        
        
        
        public TableMetadataCache(Session session) {
            this.session = session;
            this.tableMetadataCache = CacheBuilder.newBuilder().maximumSize(150).<Tablename, Metadata>build();
        }
        
        ImmutableSet<String> getColumnNames(Tablename tablename) {
            return getMetadata(tablename).getColumnNames();
        }
        
        ColumnMetadata getColumnMetadata(Tablename tablename, String columnName) {
            return getMetadata(tablename).getColumnMetadata(columnName);
        }
        
        private Metadata getMetadata(Tablename tablename) {
            Metadata metadata = tableMetadataCache.getIfPresent(tablename);
            if (metadata == null) {
                metadata = loadMetadata(tablename);
                tableMetadataCache.put(tablename, metadata);
            }
            
            return metadata;
        }
        
        
        private Metadata loadMetadata(Tablename tablename) {
            TableMetadata tableMetadata = loadTableMetadata(session, tablename);
            ImmutableSet<String> columnNames = loadColumnNames(tableMetadata);
            Metadata metadata = new Metadata(tablename, tableMetadata, columnNames);
            
            return metadata;
        }
        
        
        private static TableMetadata loadTableMetadata(Session session, Tablename tablename) {
            String keyspacename = tablename.getKeyspacename();
            if (keyspacename == null) {
                throw new IllegalStateException("no keyspacename assigned for " + tablename);
            } else {
                
                TableMetadata tableMetadata = session.getCluster().getMetadata().getKeyspace(tablename.getKeyspacename()).getTable(tablename.getTablename());
                if (tableMetadata == null) {
                    throw new RuntimeException("table " + tablename + " is not defined");
                }

                return tableMetadata;
            }
        }

        private static ImmutableSet<String> loadColumnNames(TableMetadata tableMetadata) {
            Set<String> columnNames = Sets.newHashSet();
            for (ColumnMetadata columnMetadata : tableMetadata.getColumns()) {
                columnNames.add(columnMetadata.getName());
            }
            
            return ImmutableSet.copyOf(columnNames);
        }
    }
    
    
    private static final class Metadata {
        private final Tablename tablename;
        private final TableMetadata tableMetadata;
        private final ImmutableSet<String> columnNames;
        
        public Metadata(Tablename tablename, TableMetadata tableMetadata, ImmutableSet<String> columnNames) {
            this.tablename = tablename;
            this.tableMetadata = tableMetadata;
            this.columnNames = columnNames;
        }
        
        ImmutableSet<String> getColumnNames() {
            return columnNames;
        }
        
        ColumnMetadata getColumnMetadata(String columnName) {
            ColumnMetadata metadata = tableMetadata.getColumn(columnName);
            if (metadata == null) {
                throw new RuntimeException("table " + tablename + " does not support column '" + columnName + "'");
            }
            return metadata;
        }
    }


    
    static final class UserTypeCache {
        private final Session session;
        private final Cache<String, UserType> userTypeCache;
        
        public UserTypeCache(Session session) {
            this.session = session;
            this.userTypeCache = CacheBuilder.newBuilder().maximumSize(100).<String, UserType>build();
        }

        
        public UserType get(Tablename tablename, String usertypeName) {
            String key = tablename.getKeyspacename() + "." + usertypeName;
            
            UserType userType = userTypeCache.getIfPresent(key);
            if (userType == null) {
                userType = session.getCluster().getMetadata().getKeyspace(tablename.getKeyspacename()).getUserType(usertypeName);
                userTypeCache.put(key, userType);
            } 
            
            return userType;
        }
        
        public void invalidateAll() {
            userTypeCache.invalidateAll();
        }      
    }    
}