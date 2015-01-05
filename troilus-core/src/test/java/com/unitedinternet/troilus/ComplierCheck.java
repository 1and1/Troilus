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
    
    
    private void checkDeleteWithOnlyIfDoesnotSupportCombinWith() {
        dao.deleteWhere((Clause) null)
           .onlyIf((Clause) null)
           .combinedWith(null);
    }
    
    
*/
    
   
}
    