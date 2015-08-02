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
package net.oneandone.troilus.api;



import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import net.oneandone.troilus.CassandraDB;
import net.oneandone.troilus.Dao;
import net.oneandone.troilus.DaoImpl;
import net.oneandone.troilus.Record;
import net.oneandone.troilus.Result;
import net.oneandone.troilus.example.Address;
import net.oneandone.troilus.example.AddressType;
import net.oneandone.troilus.example.ClassifierEnum;
import net.oneandone.troilus.example.Functions;
import net.oneandone.troilus.example.Hotel;
import net.oneandone.troilus.example.HotelsTable;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import com.datastax.driver.core.ColumnDefinitions.Definition;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.google.common.collect.ImmutableSet;



@Ignore
public class UserDefinedFunctionsTest {
    
    private static CassandraDB cassandra;
    
    
    @BeforeClass
    public static void beforeClass() throws IOException {
        cassandra = CassandraDB.newInstance();
    }
        
    @AfterClass
    public static void afterClass() throws IOException {
        cassandra.close();
    }

    
    @Before
    public void before() throws IOException {
        cassandra.executeCqlFile(Functions.DDL);
        cassandra.tryExecuteCqlFile(AddressType.DDL);
        cassandra.tryExecuteCqlFile(HotelsTable.DDL);
        
        cassandra.tryExecuteCqlFile(FeesTable.DDL);
    }

    
    
    @Test
    public void testBuildinFunctions() throws Exception {
        
        Dao feeDao = new DaoImpl(cassandra.getSession(), FeesTable.TABLE);

        
        CompletableFuture<Result> insert1 = feeDao.writeWithKey(FeesTable.CUSTOMER_ID, "233132", FeesTable.YEAR, 3)
                                                  .value(FeesTable.AMOUNT, 23433)
                                                  .executeAsync();
        
        CompletableFuture<Result> insert2 = feeDao.writeWithKey(FeesTable.CUSTOMER_ID, "233132", FeesTable.YEAR, 4)
                                                  .value(FeesTable.AMOUNT, 1223)
                                                  .executeAsync();

        CompletableFuture<Result> insert3 = feeDao.writeWithKey(FeesTable.CUSTOMER_ID, "233132", FeesTable.YEAR, 8)
                                                  .value(FeesTable.AMOUNT, 23233)
                                                  .executeAsync();
        
         Optional<Record> record = feeDao.readWithKey(FeesTable.CUSTOMER_ID, "233132")
                                         .column(FeesTable.YEAR) 
                                         .execute();
        
    }
     
    
    @Test
    public void testUDF() throws Exception {
        
        
        Dao hotelsDao = new DaoImpl(cassandra.getSession(), HotelsTable.TABLE);

        

        hotelsDao.writeEntity(new Hotel("BUP454554", 
                                        "Richter Panzio",
                                        ImmutableSet.of("1", "2", "3"),
                                        Optional.of(ClassifierEnum.TWO), 
                                        Optional.empty(),
                                        new Address("Thököly Ut 111", "Budapest", "1145"),
                                        Optional.of("003613568583")))
                 .execute();
        

        
        Hotel entity = new Hotel("BUP94323", 
                                 "Corinthia Budapest",
                                 ImmutableSet.of("1", "2", "3", "122", "123", "124", "322", "333"),
                                 Optional.of(ClassifierEnum.FIVE), 
                                 Optional.of("Superb hotel housed in a heritage building - exudes old world charm"),
                                 new Address("Erzsébet körút 43", "Budapest", "1073"),
                                 Optional.empty());
             
        hotelsDao.writeEntity(entity)
                 .execute();   

        
                
        ResultSet rs = cassandra.getSession().execute("SELECT phonenumber(phone) FROM " + HotelsTable.TABLE + " where id = 'BUP454554'");
        
        Row row = rs.one();
        System.out.println("***********************************************");
        System.out.println("***********************************************");
        System.out.println("***********************************************");
        
        
        for (Definition def : row.getColumnDefinitions().asList()) {
            System.out.println(def.getName() + "=" + row.getObject(def.getName()));
        }
        

        rs = cassandra.getSession().execute(QueryBuilder.select().fcall("phonenumber", QueryBuilder.raw("phone")).from(HotelsTable.TABLE));

        for (Definition def : row.getColumnDefinitions().asList()) {
            System.out.println(def.getName() + "=" + row.getObject(def.getName()));
        }
        
        System.out.println("***********************************************");
        System.out.println("***********************************************");
        System.out.println("***********************************************");
    }        
}


