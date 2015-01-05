package com.unitedinternet.troilus;

import com.datastax.driver.core.querybuilder.Clause;
import com.unitedinternet.troilus.Dao.Batchable;





public class ComplierCheck {
    
    private final Dao dao = new DaoImpl(null, null);
    
    
    // by uncommenting a method a compile error (per method) occurs

    
    
     /*
    private void checkDeleteWithOnlyIfIsNotBatchable() {
        Batchable batchable = dao.deleteWhere((Clause) null)
                                 .onlyIf(null);
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
    