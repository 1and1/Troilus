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

import com.datastax.driver.core.Session;
import com.google.common.base.Objects;


 
/**
 * Tablename
 */
public class Tablename {

    private String keyspacename;
    private String tablename;
    

    /**
     * @param session    the session
     * @param tablename  the tablename
     * @return hte tablename object
     */
    static Tablename newTablename(Session session, String tablename) {
        String keyspacename = session.getLoggedKeyspace();
        
        if (keyspacename == null) {
            throw new IllegalStateException("no keyspace assigned");
        }

        return newTablename(keyspacename, tablename);
    }
    
   
    /**
     * @param tablename    the tablename
     * @param keyspacename the keyspacename
     * @return hte tablename object
     */
    static Tablename newTablename(String keyspacename, String tablename) {
        return new Tablename(keyspacename, tablename);
    }
    
    private Tablename(String keyspacename, String tablename) {
        this.keyspacename = keyspacename;
        this.tablename = tablename;
    }

    
    /**
     * @return the keyspaceanme or null
     */
    String getKeyspacename() {
        return keyspacename;
    }
    
    /**
     * @return the tablename
     */
    public String getTablename() {
        return tablename;
    }
    
    @Override
    public boolean equals(Object other) {
        return (other instanceof Tablename) && 
                Objects.equal(((Tablename) other).keyspacename, this.keyspacename) &&
                Objects.equal(((Tablename) other).tablename, this.tablename);
    }
    
    @Override
    public int hashCode() {
        return toString().hashCode();
    }
    
    @Override
    public String toString() {
        return (keyspacename == null) ? tablename : keyspacename + "." + tablename;
    }
}