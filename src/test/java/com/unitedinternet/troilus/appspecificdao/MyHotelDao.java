package com.unitedinternet.troilus.appspecificdao;





import com.datastax.driver.core.ConsistencyLevel;
import com.unitedinternet.troilus.Dao;


public interface MyHotelDao extends Dao {
    
    MyHotelDao withReferentialIntegrityCheck();
    
    MyHotelDao withConsistency(ConsistencyLevel consistencyLevel);
}


