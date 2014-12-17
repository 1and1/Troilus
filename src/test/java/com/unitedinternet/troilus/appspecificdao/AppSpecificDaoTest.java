package com.unitedinternet.troilus.appspecificdao;





import org.junit.Test;

import com.datastax.driver.core.ConsistencyLevel;
import com.unitedinternet.troilus.AbstractCassandraBasedTest;


public class AppSpecificDaoTest extends AbstractCassandraBasedTest {
    

        
    
    @Test
    public void testSimpleTable() throws Exception {
        MyDaoManager myDaoManager = new MyDaoManager(getSession());

/*        MyHotelDao hotelDao = myDaoManager.getMyHotelDao()
                                          .withReferentialIntegrityCheck()
                                          .withConsistency(ConsistencyLevel.ONE);*/
    }               
}


