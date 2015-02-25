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
import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.Instant;
import java.util.Iterator;
import java.util.Optional;

import net.oneandone.troilus.AbstractCassandraBasedTest;
import net.oneandone.troilus.Batchable;
import net.oneandone.troilus.Count;
import net.oneandone.troilus.Dao;
import net.oneandone.troilus.DaoImpl;
import net.oneandone.troilus.IfConditionException;
import net.oneandone.troilus.Record;
import net.oneandone.troilus.CounterMutation;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.ExecutionInfo;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;


public class ColumnsApiTest extends AbstractCassandraBasedTest {
    

    @Before
    public void before() throws IOException {
        tryExecuteCqlFile(UsersTable.DDL);
        tryExecuteCqlFile(LoginsTable.DDL);
        tryExecuteCqlFile(PlusLoginsTable.DDL);
    }

    
    @Test
    public void testSimpleTable() throws Exception {
        Dao usersDao = new DaoImpl(getSession(), UsersTable.TABLE)
                                 .withConsistency(ConsistencyLevel.LOCAL_QUORUM);

        Dao loginsDao = new DaoImpl(getSession(), LoginsTable.TABLE)
                                .withConsistency(ConsistencyLevel.LOCAL_QUORUM);
   
        Dao plusLoginsDao = new DaoImpl(getSession(), PlusLoginsTable.TABLE)
                                .withConsistency(ConsistencyLevel.LOCAL_QUORUM);

        

        Count num = usersDao.readAll()
                            .count()
                            .execute();
        Assert.assertEquals(0, num.getCount());
        


        
        ////////////////
        // inserts
        usersDao.writeWithKey(UsersTable.USER_ID, "95454")
                .value(UsersTable.IS_CUSTOMER, true) 
                .value(UsersTable.PICTURE, ByteBuffer.wrap(new byte[] { 8, 4, 3})) 
                .value(UsersTable.ADDRESSES, ImmutableList.of("stuttgart", "baden-baden")) 
                .value(UsersTable.PHONE_NUMBERS, ImmutableSet.of("34234243", "9345324"))
                .execute();
        
        
        ExecutionInfo info =  usersDao.writeWithKey(UsersTable.USER_ID, "8345345")
                                      .value(UsersTable.PHONE_NUMBERS, ImmutableSet.of("24234244"))
                                      .value(UsersTable.IS_CUSTOMER, true)
                                      .ifNotExists()
                                      .withTtl(Duration.ofMinutes(2))
                                      .withWritetime(Instant.now().toEpochMilli() * 1000)
                                      .withTracking()
                                      .execute()
                                      .getExecutionInfo();

        Assert.assertNotNull(info.getQueryTrace());
        
        
        
        usersDao.writeWithKey(UsersTable.USER_ID, "4545")
                .value(UsersTable.IS_CUSTOMER, true)
                .value(UsersTable.PICTURE, ByteBuffer.wrap(new byte[] { 4, 5, 5}))
                .value(UsersTable.ADDRESSES, ImmutableList.of("münchen", "karlsruhe"))
                .value(UsersTable.PHONE_NUMBERS, ImmutableSet.of("94665", "34324543"))
                .ifNotExists()
                .withTracking()
                .execute();


        try {   // insert twice!
            usersDao.writeWithKey(UsersTable.USER_ID, "4545")
                    .value(UsersTable.IS_CUSTOMER, true)
                    .value(UsersTable.PICTURE, ByteBuffer.wrap(new byte[] { 4, 5, 5}))
                    .value(UsersTable.ADDRESSES, ImmutableList.of("münchen", "karlsruhe"))
                    .value(UsersTable.PHONE_NUMBERS, ImmutableSet.of("94665", "34324543"))
                    .ifNotExists()
                    .withTtl(Duration.ofHours(1))
                    .execute();
            
            Assert.fail("DuplicateEntryException expected"); 
        } catch (IfConditionException expected) { }  

        
        
        usersDao.writeWithKey(UsersTable.USER_ID, "3434343")
                .value(UsersTable.IS_CUSTOMER, Optional.of(true))
                .value(UsersTable.PICTURE, ByteBuffer.wrap(new byte[] { 4, 5, 5}))
                .value(UsersTable.ADDRESSES, null)
                .value(UsersTable.PHONE_NUMBERS, Optional.empty())
                .execute();


        
        ////////////////
        // reads
        Optional<Record> optionalRecord = usersDao.readWithKey(UsersTable.USER_ID, "4545")
                                                  .column(UsersTable.PICTURE)
                                                  .column(UsersTable.ADDRESSES)
                                                  .column(UsersTable.PHONE_NUMBERS)
                                                  .execute();
        Assert.assertTrue(optionalRecord.isPresent());
        optionalRecord.ifPresent(record -> System.out.println(record.getList(UsersTable.ADDRESSES, String.class)));
        System.out.println(optionalRecord.get());
     
        
        
        
        
        
        Optional<Record> optionalRecord2 = usersDao.readWithKey(UsersTable.USER_ID, "95454")
                                                   .columns(UsersTable.PICTURE, UsersTable.ADDRESSES, UsersTable.PHONE_NUMBERS)
                                                   .execute();
        Assert.assertTrue(optionalRecord2.isPresent());
        optionalRecord2.ifPresent(record -> System.out.println(record.getList(UsersTable.ADDRESSES, String.class)));

 
        
        Optional<Record> optionalRecord3 = usersDao.readWithKey(UsersTable.USER_ID, "8345345")
                                                   .columnWithMetadata(UsersTable.IS_CUSTOMER)
                                                   .column(UsersTable.PICTURE)
                                                   .execute();
        Assert.assertTrue(optionalRecord3.isPresent());
        optionalRecord3.ifPresent(record -> System.out.println(record));

 

        num = usersDao.readAll()
                      .count()
                      .execute();
        Assert.assertEquals(4,  num.getCount());
        
        

        ////////////////
        // deletes
        usersDao.deleteWithKey(UsersTable.USER_ID, "4545")
                .execute();
        
        // check
        optionalRecord = usersDao.readWithKey(UsersTable.USER_ID, "4545")
                                 .column(UsersTable.USER_ID)
                                 .execute();
        Assert.assertFalse(optionalRecord.isPresent());
        

        
        
        
        
        
        //////////////// 
        // batch inserts
        Batchable<?> insert1 = usersDao.writeWithKey(UsersTable.USER_ID, "14323425")
                                       .value(UsersTable.IS_CUSTOMER, true)
                                       .value(UsersTable.ADDRESSES, ImmutableList.of("berlin", "budapest"))
                                       .value(UsersTable.PHONE_NUMBERS, ImmutableSet.of("12313241243", "232323"));
        
        
        Batchable<?> insert2 = usersDao.writeWithKey(UsersTable.USER_ID, "2222")
                                       .value(UsersTable.IS_CUSTOMER, true)
                                       .value(UsersTable.ADDRESSES, ImmutableList.of("berlin", "budapest"))
                                       .value(UsersTable.PHONE_NUMBERS, ImmutableSet.of("12313241243", "232323"));
        
        usersDao.writeWithKey(UsersTable.USER_ID, "222222")
                .value(UsersTable.IS_CUSTOMER, true)
                .value(UsersTable.ADDRESSES, ImmutableList.of("hamburg"))
                .value(UsersTable.PHONE_NUMBERS, ImmutableSet.of("945453", "23432234"))
                .combinedWith(insert1)
                .combinedWith(insert2)
                .withWriteAheadLog()
                .execute();
        
        
        // check
        optionalRecord = usersDao.readWithKey(UsersTable.USER_ID, "14323425")
                                 .all()
                                 .execute();
        System.out.println(optionalRecord);
        Assert.assertTrue(optionalRecord.isPresent());
        
        
        Record record = usersDao.readWithKey(UsersTable.USER_ID, "8345345")
                                .execute()
                                .get();
        Assert.assertEquals("8345345", record.getString(UsersTable.USER_ID));
        Assert.assertEquals(true, record.getBool(UsersTable.IS_CUSTOMER));
        
        Iterator<String> phoneNumbers = record.getSet(UsersTable.PHONE_NUMBERS, String.class).iterator();
        Assert.assertEquals("24234244", phoneNumbers.next());
        Assert.assertFalse(phoneNumbers.hasNext());
        
        
        
        // Inc/Set counter value 
        loginsDao.writeWithKey(LoginsTable.USER_ID, "8345345")
                .incr(LoginsTable.LOGINS)
                .execute();
        
        record = loginsDao.readWithKey(LoginsTable.USER_ID, "8345345")
                          .execute()
                          .get();
        Assert.assertEquals(1, record.getLong(LoginsTable.LOGINS));
        
        
        loginsDao.writeWithKey(LoginsTable.USER_ID, "8345345")
                 .incr(LoginsTable.LOGINS, 4)   
                 .execute();

        record = loginsDao.readWithKey(LoginsTable.USER_ID, "8345345")
                          .execute()
                          .get();
        Assert.assertEquals(5, record.getLong(LoginsTable.LOGINS));

        
        
        loginsDao.writeWithKey(LoginsTable.USER_ID, "8345345")
                 .decr(LoginsTable.LOGINS)   
                 .execute();

        record = loginsDao.readWithKey(LoginsTable.USER_ID, "8345345")
                          .execute()
                          .get();
        Assert.assertEquals(4, record.getLong(LoginsTable.LOGINS));


        
        loginsDao.writeWithKey(LoginsTable.USER_ID, "8345345")
                 .decr(LoginsTable.LOGINS)
                 .withConsistency(ConsistencyLevel.LOCAL_QUORUM)
                 .execute();

        record = loginsDao.readWithKey(LoginsTable.USER_ID, "8345345")
                          .execute()
                          .get();
        Assert.assertEquals(3, record.getLong(LoginsTable.LOGINS));



        
        loginsDao.writeWhere(QueryBuilder.eq(LoginsTable.USER_ID, "8345345"))
                 .incr(LoginsTable.LOGINS)
                 .withConsistency(ConsistencyLevel.LOCAL_QUORUM)
                 .execute();

        record = loginsDao.readWithKey(LoginsTable.USER_ID, "8345345")
                          .execute()
                          .get();
        Assert.assertEquals(4, record.getLong(LoginsTable.LOGINS));


        
        plusLoginsDao.writeWhere(QueryBuilder.eq(PlusLoginsTable.USER_ID, "8345345"))
                     .incr(PlusLoginsTable.LOGINS)
                     .withWritetime(Instant.now().toEpochMilli() * 1000)
                     .withConsistency(ConsistencyLevel.LOCAL_QUORUM)
                     .execute();
        
        record = plusLoginsDao.readWithKey(PlusLoginsTable.USER_ID, "8345345")
                          .execute()
                          .get();
        Assert.assertEquals(1, record.getLong(PlusLoginsTable.LOGINS));
        
        
        

        CounterMutation cm = loginsDao.writeWhere(QueryBuilder.eq(LoginsTable.USER_ID, "8345345"))
                                      .incr(LoginsTable.LOGINS);

        plusLoginsDao.writeWhere(QueryBuilder.eq(PlusLoginsTable.USER_ID, "8345345"))
                     .incr(PlusLoginsTable.LOGINS)
                     .withWritetime(Instant.now().toEpochMilli() * 1000)
                     .withConsistency(ConsistencyLevel.LOCAL_QUORUM)
                     .combinedWith(cm)
                     .execute();

        
        record = plusLoginsDao.readWithKey(PlusLoginsTable.USER_ID, "8345345")
                .execute()
                .get();
        Assert.assertEquals(2, record.getLong(PlusLoginsTable.LOGINS));

        record = loginsDao.readWithKey(LoginsTable.USER_ID, "8345345")
                .execute()
                .get();
        Assert.assertEquals(5, record.getLong(LoginsTable.LOGINS));


        
        
        
        
        
        
        
        // remove value
        usersDao.writeWithKey(UsersTable.USER_ID, "8345345")
                .value(UsersTable.IS_CUSTOMER, null)
                .execute();
        
    
        record = usersDao.readWithKey(UsersTable.USER_ID, "8345345")
                         .execute()
                         .get();
        Assert.assertEquals("8345345", record.getString(UsersTable.USER_ID));
        Assert.assertFalse(record.getBool(UsersTable.IS_CUSTOMER));

        phoneNumbers = record.getSet(UsersTable.PHONE_NUMBERS, String.class).iterator();
        Assert.assertEquals("24234244", phoneNumbers.next());
        Assert.assertFalse(phoneNumbers.hasNext());
        
        
        
        
        

        
        ////////////////////
        // conditional update

        
        try {
            usersDao.writeWithKey(UsersTable.USER_ID, "2222")
                    .value(UsersTable.ADDRESSES, ImmutableList.of("nürnberg"))
                    .onlyIf(QueryBuilder.eq(UsersTable.IS_CUSTOMER, false))
                    .withSerialConsistency(ConsistencyLevel.SERIAL)
                    .execute();
            Assert.fail("IfConditionException expected");
        } catch (IfConditionException expected) {  }

        record = usersDao.readWithKey(UsersTable.USER_ID, "2222")
                .execute()
                .get();
        
        Iterator<String> addresses= record.getList(UsersTable.ADDRESSES, String.class).iterator();
        Assert.assertEquals("berlin", addresses.next());
        Assert.assertEquals("budapest", addresses.next());
        Assert.assertFalse(addresses.hasNext());        

        
        
        usersDao.writeWithKey(UsersTable.USER_ID, "2222")
                .value(UsersTable.ADDRESSES, ImmutableList.of("nürnberg"))
                .onlyIf(QueryBuilder.eq(UsersTable.IS_CUSTOMER, true))
                .execute();

        record = usersDao.readWithKey(UsersTable.USER_ID, "2222")
                .execute()
                .get();
        
        addresses= record.getList(UsersTable.ADDRESSES, String.class).iterator();
        Assert.assertEquals("nürnberg", addresses.next());
        Assert.assertFalse(addresses.hasNext());   
        
        
        
        
        
        usersDao.writeWhere(QueryBuilder.in(UsersTable.USER_ID, "2222"))
                .value(UsersTable.ADDRESSES, ImmutableList.of("berlin", "budapest"))
                .execute();
        
        record = usersDao.readWithKey(UsersTable.USER_ID, "2222")
                .execute()
                .get();
        
        addresses= record.getList(UsersTable.ADDRESSES, String.class).iterator();
        Assert.assertEquals("berlin", addresses.next());
        Assert.assertEquals("budapest", addresses.next());
        Assert.assertFalse(addresses.hasNext());        
        
        
        
        
        
        try {
            usersDao.deleteWithKey(UsersTable.USER_ID, "2222")
                    .onlyIf(QueryBuilder.eq(UsersTable.IS_CUSTOMER, false))
                    .execute();
            Assert.fail("IfConditionException expected");
        } catch (IfConditionException expected) { }

        Optional<Record> rec = usersDao.readWithKey(UsersTable.USER_ID, "2222")
                                       .execute();
        Assert.assertTrue(rec.isPresent());   

        
        usersDao.deleteWithKey(UsersTable.USER_ID, "2222")
                .onlyIf(QueryBuilder.eq(UsersTable.IS_CUSTOMER, true))
                .execute();

        rec = usersDao.readWithKey(UsersTable.USER_ID, "2222")
                      .execute();
        Assert.assertFalse(rec.isPresent());   
        
        
        
        
        
        
        
        usersDao.writeWithKey(UsersTable.USER_ID, "8345345")
                .value(UsersTable.IS_CUSTOMER, true)
                .value(UsersTable.ADDRESSES, ImmutableList.of("hamburg"))
                .execute();
        
        record = usersDao.readWithKey(UsersTable.USER_ID, "8345345")
                .execute()
                .get();
        Assert.assertEquals("8345345", record.getString(UsersTable.USER_ID));
        Assert.assertTrue(record.getBool(UsersTable.IS_CUSTOMER));
        Assert.assertFalse(record.getList(UsersTable.ADDRESSES, String.class).isEmpty());
        Assert.assertFalse(record.getSet(UsersTable.PHONE_NUMBERS, String.class).isEmpty());
        


        
        
        
        // remove value
        usersDao.writeWithKey(UsersTable.USER_ID, "8345345")
                .value(UsersTable.IS_CUSTOMER, null)
                .value(UsersTable.ADDRESSES, null)
                .execute();        
        
        
        record = usersDao.readWithKey(UsersTable.USER_ID, "8345345")
                         .execute()
                         .get();
        Assert.assertEquals("8345345", record.getString(UsersTable.USER_ID));
        Assert.assertFalse(record.getBool(UsersTable.IS_CUSTOMER));
        Assert.assertTrue(record.getList(UsersTable.ADDRESSES, String.class).isEmpty());

        
        
        // add set value
        usersDao.writeWithKey(UsersTable.USER_ID, "8345345")
                .addSetValue(UsersTableFields.PHONE_NUMBERS, "12142343")
                .execute();        
        
        
        record = usersDao.readWithKey(UsersTable.USER_ID, "8345345")
                         .execute()
                         .get();
        Assert.assertEquals("8345345", record.getString(UsersTable.USER_ID));
        Assert.assertFalse(record.getBool(UsersTable.IS_CUSTOMER));
        Assert.assertTrue(record.getSet(UsersTable.PHONE_NUMBERS, String.class).contains("12142343"));
        
        
        
        
        // add set value
        usersDao.writeWithKey(UsersTable.USER_ID, "8345345")
                .addSetValue(UsersTable.PHONE_NUMBERS, "23234234")
                .execute();        
        
        
        record = usersDao.readWithKey(UsersTable.USER_ID, "8345345")
                         .execute()
                         .get();
        Assert.assertEquals("8345345", record.getString(UsersTable.USER_ID));
        Assert.assertFalse(record.getBool(UsersTable.IS_CUSTOMER));
        Assert.assertTrue(record.getSet(UsersTable.PHONE_NUMBERS, String.class).contains("12142343"));
        Assert.assertTrue(record.getSet(UsersTable.PHONE_NUMBERS, String.class).contains("23234234"));
        
        
        // remove set value
        usersDao.writeWithKey(UsersTable.USER_ID, "8345345")
                .removeSetValue(UsersTable.PHONE_NUMBERS, "12142343")
                .execute();        
        
        
        record = usersDao.readWithKey(UsersTable.USER_ID, "8345345")
                         .execute()
                         .get();
        Assert.assertEquals("8345345", record.getString(UsersTable.USER_ID));
        Assert.assertFalse(record.getBool(UsersTable.IS_CUSTOMER));
        Assert.assertFalse(record.getSet(UsersTable.PHONE_NUMBERS, String.class).contains("12142343"));
        Assert.assertTrue(record.getSet(UsersTable.PHONE_NUMBERS, String.class).contains("23234234"));
        Assert.assertTrue(record.getList(UsersTable.ADDRESSES, String.class).isEmpty());
        
        
        
        
        // add list value
        usersDao.writeWithKey(UsersTable.USER_ID, "8345345")
                .value(UsersTable.ADDRESSES, ImmutableList.of("bonn", "stuttgart"))
                .execute();        
        
        
        record = usersDao.readWithKey(UsersTable.USER_ID, "8345345")
                         .execute()
                         .get();
        Assert.assertEquals("8345345", record.getString(UsersTable.USER_ID));
        Assert.assertFalse(record.getBool(UsersTable.IS_CUSTOMER));
        Iterator<String> addrIt = record.getList(UsersTable.ADDRESSES, String.class).iterator();
        Assert.assertEquals("bonn", addrIt.next());
        Assert.assertEquals("stuttgart", addrIt.next());
        Assert.assertFalse(addrIt.hasNext());
   
        
        
        
        // modify list value
        usersDao.writeWithKey(UsersTable.USER_ID, "8345345")
                .prependListValue(UsersTable.ADDRESSES, "bern")
                .prependListValue(UsersTable.ADDRESSES, "neustadt")
                .prependListValue(UsersTable.ADDRESSES, "ulm")
                .appendListValue(UsersTable.ADDRESSES, "frankfurt")
                .appendListValue(UsersTable.ADDRESSES, "innsbruck")
                .removeListValue(UsersTable.ADDRESSES, "bonn")
                .appendListValue(UsersTable.ADDRESSES, "berlin")
                .execute();        
        
        
        record = usersDao.readWithKey(UsersTable.USER_ID, "8345345")
                         .execute()
                         .get();
        Assert.assertEquals("8345345", record.getString(UsersTable.USER_ID));
        Assert.assertFalse(record.getBool(UsersTable.IS_CUSTOMER));
        addrIt = record.getList(UsersTable.ADDRESSES, String.class).iterator();
        Assert.assertEquals("ulm", addrIt.next());
        Assert.assertEquals("neustadt", addrIt.next());
        Assert.assertEquals("bern", addrIt.next());
        Assert.assertEquals("stuttgart", addrIt.next());
        Assert.assertEquals("frankfurt", addrIt.next());
        Assert.assertEquals("innsbruck", addrIt.next());
        Assert.assertEquals("berlin", addrIt.next());
        Assert.assertFalse(addrIt.hasNext());
   
        
        
        
        
        
        // set map
        usersDao.writeWithKey(UsersTable.USER_ID, "8345345")
                .value(UsersTable.ROLES, ImmutableMap.of("customer", "xe333"))
                .execute();        
        
        
        record = usersDao.readWithKey(UsersTable.USER_ID, "8345345")
                         .execute()
                         .get();
        Assert.assertEquals("8345345", record.getString(UsersTable.USER_ID));
        Assert.assertEquals("xe333", record.getMap(UsersTable.ROLES, String.class, String.class).get("customer"));
        
        
        
        
        usersDao.writeWithKey(UsersTable.USER_ID, "8345345")
                .putMapValue(UsersTable.ROLES, "player_type1", "p14334")
                .putMapValue(UsersTable.ROLES, "player_type2", "p233")
                .execute();        
        
        record = usersDao.readWithKey(UsersTable.USER_ID, "8345345")
                         .execute()
                         .get();
        Assert.assertEquals("xe333", record.getMap(UsersTable.ROLES, String.class, String.class).get("customer"));
        Assert.assertEquals("p14334", record.getMap(UsersTable.ROLES, String.class, String.class).get("player_type1"));
        Assert.assertEquals("p233", record.getMap(UsersTable.ROLES, String.class, String.class).get("player_type2"));


        
   /* does not work
        usersDao.writeWithKey(UsersTable.USER_ID, "8345345")
                .putMapValue(UsersTable.ROLES, "player_type1", null)
                .execute();        

        record = usersDao.readWithKey(UsersTable.USER_ID, "8345345")
                         .execute()
                         .get();
                         */
        
        
        
        usersDao.writeWithKey(UsersTableFields.USER_ID, "343434")
                .value(UsersTableFields.IS_CUSTOMER, true)
                .value(UsersTable.ROLES, ImmutableMap.of("customer", "xe333"))
                .value(UsersTableFields.PICTURE, new byte[] { 4, 5, 5})
                .value(UsersTableFields.ADDRESSES, ImmutableList.of("münchen", "karlsruhe"))
                .value(UsersTableFields.PHONE_NUMBERS, ImmutableSet.of("94665", "34324543"))
                .ifNotExists()
                .withTracking()
                .execute();

        record = usersDao.readWithKey(UsersTableFields.USER_ID, "343434")
                .execute()
                .get();
        
        Assert.assertEquals("343434", record.getValue(UsersTableFields.USER_ID));
        Assert.assertNotNull(record.getValue(UsersTableFields.IS_CUSTOMER));
        Assert.assertFalse(record.getValue(UsersTableFields.PHONE_NUMBERS).contains("12142343"));
        Assert.assertTrue(record.getValue(UsersTableFields.PHONE_NUMBERS).contains("34324543"));
        Assert.assertTrue(record.getValue(UsersTableFields.ADDRESSES).contains("karlsruhe"));
        Assert.assertEquals("xe333", record.getValue(UsersTableFields.ROLES).get("customer"));
        Assert.assertArrayEquals(new byte[] { 4, 5, 5}, record.getValue(UsersTableFields.PICTURE));
        
        
        
        
        
   
        
        usersDao.writeWithKey(UsersTableFields.USER_ID, "45436")
                .value(UsersTableFields.PHONE_NUMBERS, ImmutableSet.of("24234244"))
                .value(UsersTableFields.IS_CUSTOMER, true)
                .value(UsersTableFields.USER_TYPE, UserType.METAL)
                .execute();

        Assert.assertNotNull(info.getQueryTrace());
        
        
        
        record = usersDao.readWithKey(UsersTableFields.USER_ID, "45436")
                .execute()
                .get();
        
        Assert.assertEquals(UserType.METAL, record.getValue(UsersTableFields.USER_TYPE));
        
        
        
        record = usersDao.withTracking().readWithKey(UsersTableFields.USER_ID, "45436")
                         .withConsistency(ConsistencyLevel.ALL)
                         .execute()
                         .get();
        
        Assert.assertEquals(UserType.METAL, record.getValue(UsersTableFields.USER_TYPE));
        Assert.assertNotNull(record.getExecutionInfo().getQueryTrace());
  //      Assert.assertTrue(record.toString().contains("Merging memtable tombstones"));
        
        record = usersDao.withoutTracking()
                         .readWithKey(UsersTableFields.USER_ID, "45436")
                         .execute()
                         .get();
        
        Assert.assertEquals(UserType.METAL, record.getValue(UsersTableFields.USER_TYPE));
        Assert.assertNull(record.getExecutionInfo().getQueryTrace());
        
        
        
        usersDao.writeWithKey(UsersTable.USER_ID, "23452342342")
                .value(UsersTable.IS_CUSTOMER, true) 
                .execute();

        record = usersDao.readWithKey(UsersTableFields.USER_ID, "23452342342")
                .execute()
                .get();

        Assert.assertTrue(record.getValue(UsersTableFields.PHONE_NUMBERS).isEmpty());
        Assert.assertTrue(record.getValue(UsersTableFields.ADDRESSES).isEmpty());
        Assert.assertArrayEquals(new byte[0], record.getValue(UsersTableFields.PICTURE));
        Assert.assertNull(record.getValue(UsersTableFields.USER_TYPE));
        
        
        
        record = usersDao.readWithKey(UsersTableFields.USER_ID, "23452342342")
                         .column(UsersTable.USER_TYPE)
                         .execute()
                         .get();

        try {
            Assert.assertNull(record.getBool(UsersTableFields.IS_CUSTOMER.getName()));
            Assert.fail("IllegalArgumentException expected");
        } catch (IllegalArgumentException expected) { }
        
        
        
        usersDao.writeWithKey(UsersTable.USER_ID, "5553344")
                .value(UsersTable.IS_CUSTOMER, true) 
                .withTtl(Duration.ofSeconds(1))
                .execute();
        
        
        
   
        Batchable<?> insert5 = usersDao.writeWithKey(UsersTable.USER_ID, "234234234424")
                                    .value(UsersTable.IS_CUSTOMER, true)
                                    .value(UsersTable.ADDRESSES, ImmutableList.of("berlin", "budapest"))
                                    .value(UsersTable.PHONE_NUMBERS, ImmutableSet.of("12313241243", "232323"));
        
        usersDao.writeWithKey(UsersTable.USER_ID, "235423423424")
                .value(UsersTable.IS_CUSTOMER, true)
                .value(UsersTable.ADDRESSES, ImmutableList.of("hamburg"))
                .value(UsersTable.PHONE_NUMBERS, ImmutableSet.of("945453", "23432234"))
                .combinedWith(insert5)
                .withoutWriteAheadLog()
                .withConsistency(ConsistencyLevel.QUORUM)
                .execute();
        
        
        

        Batchable<?> w1 = usersDao.writeWithKey(UsersTable.USER_ID, "456456645243245")
                               .value(UsersTable.IS_CUSTOMER, true)
                               .value(UsersTable.ADDRESSES, ImmutableList.of("berlin", "budapest"))
                               .value(UsersTable.PHONE_NUMBERS, ImmutableSet.of("12313241243", "232323"));
        
        Batchable<?> w2 = usersDao.writeWithKey(UsersTable.USER_ID, "456456645243245")
                               .value(UsersTable.IS_CUSTOMER, true)
                               .value(UsersTable.ADDRESSES, ImmutableList.of("berlin", "budapest"))
                               .value(UsersTable.PHONE_NUMBERS, ImmutableSet.of("12313241243", "232323"));
        
        Batchable<?> batch = w1.combinedWith(w2)
                              .withTracking();
        batch.execute();
        
        
        record = usersDao.readWithKey(UsersTableFields.USER_ID, "456456645243245")
                .execute()
                .get();
        Assert.assertEquals(true, record.getValue(UsersTableFields.IS_CUSTOMER));
    }        
}


