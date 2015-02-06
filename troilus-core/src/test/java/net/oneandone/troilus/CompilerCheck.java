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

import java.time.Duration;

import com.datastax.driver.core.querybuilder.Clause;

import net.oneandone.troilus.Dao;
import net.oneandone.troilus.Batchable;
import net.oneandone.troilus.DaoImpl;





@SuppressWarnings("unused")
public class CompilerCheck {
    
    private final Dao dao = new DaoImpl(null, null);
    
    
    // by uncommenting a method a compile error (per method) occurs

    /*
    
    private void checkDeleteWithOnlyIfIsNotBatchable() {
        Deletion deletion = dao.deleteWhere((Clause) null);
        Batchable batchable = deletion.onlyIf(null);
    }
    
    private void checkDeleteDoesNotSupportTTL() {
        Deletion deletion = dao.deleteWhere((Clause) null);
        deletion.withTtl(Duration.ofSeconds(1));
    }

    private void checkDeleteDoesNotSupportWritetime() {
        Deletion deletion = dao.deleteWhere((Clause) null);
        deletion.withWritetime(3234324);
    }
    
    private void checkDeleteWithOnlyIfDoesNotSupportCombinWith() {
        dao.deleteWhere((Clause) null)
           .onlyIf((Clause) null)
           .combinedWith(null);
    }

    private void checkWriteEntityDoesNotSupportValueMethod() {
        dao.writeEntity(null)
           .value("name", "value");
    }
    
    private void checkWriteEntityDoesNotSupportOnlyIf() {
        dao.writeEntity(null)
           .onlyIf((Clause) null);
    }
    
    private void checkWriteEntityWithIfNotExistsIsNotBatchable() {
        Batchable batchable = dao.writeEntity(null)
                                 .ifNotExits();
    }

    private void checkWriteEntityWithIfDoesNotSupportCombinWith() {
        dao.writeEntity(null)
           .ifNotExits()
           .combinedWith(null);
    }

    private void checkWriteWithKeyWithIfNotExistsIsNotBatchable() {
        Batchable batchable = dao.writeWithKey("keyName", "keyValue")
                                 .ifNotExits();
    }
    
    private void checkWriteWithKeyWithIfDoesNotSupportCombinWith() {
        dao.writeWithKey("keyName", "keyValue")
           .ifNotExits()
           .combinedWith(null);
    }
    
    */
}
    