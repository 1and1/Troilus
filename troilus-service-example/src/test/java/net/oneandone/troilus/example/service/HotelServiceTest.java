package net.oneandone.troilus.example.service;

import java.io.File;
import java.util.Optional;

import javax.ws.rs.client.Client;










import net.oneandone.troilus.Dao;
import net.oneandone.troilus.DaoImpl;
import net.oneandone.troilus.example.HotelsTable;
import net.oneandone.troilus.example.service.Hotel;
import net.oneandone.troilus.example.service.HotelRepresentation;




import org.apache.catalina.startup.Tomcat;
import org.jboss.resteasy.client.jaxrs.ResteasyClientBuilder;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import com.datastax.driver.core.ConsistencyLevel;
import com.google.common.collect.ImmutableSet;

public class HotelServiceTest extends AbstractCassandraBasedTest {
    
    
    @Ignore    
    @Test
    public void testExample() throws Exception {
        filldb();
        
        Tomcat server = new Tomcat();
        server.setPort(9080);
        server.addWebapp("/service", new File("src/main/resources/webapp").getAbsolutePath());
        server.start();
        
        Client client =  ResteasyClientBuilder.newClient();

        
        HotelRepresentation hotel = client.target("http://localhost:9080/service/hotels/BUP45544")
                                          .request()
                                          .get(HotelRepresentation.class);
        Assert.assertEquals("Corinthia Budapest", hotel.getName());
    }        

    
    
    
    private void filldb() {
        Dao hotelsDao = new DaoImpl(getSession(), HotelsTable.TABLE);
        
        hotelsDao.writeEntity(new Hotel("BUP45544", 
                                        "Corinthia Budapest",
                                        ImmutableSet.of("1", "2", "3", "122", "123", "124", "322", "333"),
                                        Optional.of(5), 
                                        Optional.of("Superb hotel housed in a heritage building - exudes old world charm")
                                       ))
                 .execute();

  
        
        
        hotelsDao.writeWithKey("id", "BUP932432")
                 .value("name", "City Budapest")
                 .value("room_ids", ImmutableSet.of("1", "2", "3", "4", "5"))
                 .value("classification", 4)
                 .execute();

    
        hotelsDao.writeEntity(new Hotel("BUP14334", 
                                        "Richter Panzio",
                                        ImmutableSet.of("1", "2", "3"),
                                        Optional.of(2), 
                                        Optional.empty())
                                        )
                  .withConsistency(ConsistencyLevel.ANY)
                  .execute();
    }
}


