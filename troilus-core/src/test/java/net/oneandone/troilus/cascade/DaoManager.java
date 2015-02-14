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
package net.oneandone.troilus.cascade;

import com.datastax.driver.core.Session;

import net.oneandone.troilus.Dao;
import net.oneandone.troilus.DaoImpl;


public class DaoManager  {

    private final Dao keyByAccountDao;
    private final Dao keyByEmailDao;

    
    public DaoManager(Session session) {
        Dao keyByAccountDao = new DaoImpl(session, KeyByAccountColumns.TABLE);
        this.keyByEmailDao = new DaoImpl(session, KeyByEmailColumns.TABLE);
        
        this.keyByAccountDao = keyByAccountDao.withInterceptor(new KeyByAccountColumns.CascadeByEmailDao(keyByAccountDao, keyByEmailDao));
    }
    
    
    public Dao getKeyByAccountDao() {
        return keyByAccountDao;
    }
    
    public Dao getKeyByEmailDao() {
        return keyByEmailDao;
    }
}

