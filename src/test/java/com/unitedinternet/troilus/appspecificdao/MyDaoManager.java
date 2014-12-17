package com.unitedinternet.troilus.appspecificdao;




import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.Session;
import com.unitedinternet.troilus.Context;
import com.unitedinternet.troilus.DaoImpl;




public class MyDaoManager {
    
    private final Session session;

    public MyDaoManager(Session session) {
        this.session = session;
    }
    
    
    public MyHotelDao getMyHotelDao() {
        return new MyHotelDaoImpl(new Context(session, "hotel"));
    }
    
    
    private static class MyHotelDaoImpl extends DaoImpl implements MyHotelDao {
     
        public MyHotelDaoImpl(Context context) {
            super(context);
        }
        
        @Override
        public MyHotelDao withReferentialIntegrityCheck() {
            return new MyHotelDaoImpl(getDefaultContext());
        }
        

        @Override
        public MyHotelDao withConsistency(ConsistencyLevel consistencyLevel) {
            return new MyHotelDaoImpl(getDefaultContext().withConsistency(consistencyLevel));
        }
    }
    
}


