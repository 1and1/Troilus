package net.oneandone.troilus.api;




import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.Instant;
import java.util.Iterator;
import java.util.Optional;

import net.oneandone.troilus.AbstractCassandraBasedTest;
import net.oneandone.troilus.ConstraintException;
import net.oneandone.troilus.Count;
import net.oneandone.troilus.Dao;
import net.oneandone.troilus.DaoImpl;
import net.oneandone.troilus.Record;
import net.oneandone.troilus.Result;
import net.oneandone.troilus.Dao.Batchable;
import net.oneandone.troilus.Dao.CounterMutation;

import org.junit.Assert;
import org.junit.Test;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.ExecutionInfo;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;


public class ColumnsApiTest extends AbstractCassandraBasedTest {
    

        
    
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
                                      .withEnableTracking()
                                      .execute()
                                      .getExecutionInfo();

        Assert.assertNotNull(info.getQueryTrace());
        
        
        
        Result result = usersDao.writeWithKey(UsersTable.USER_ID, "4545")
                                .value(UsersTable.IS_CUSTOMER, true)
                                .value(UsersTable.PICTURE, ByteBuffer.wrap(new byte[] { 4, 5, 5}))
                                .value(UsersTable.ADDRESSES, ImmutableList.of("münchen", "karlsruhe"))
                                .value(UsersTable.PHONE_NUMBERS, ImmutableSet.of("94665", "34324543"))
                                .ifNotExists()
                                .withEnableTracking()
                                .execute();


        try {   // insert twice!
            usersDao.writeWithKey(UsersTable.USER_ID, "4545")
                    .value(UsersTable.IS_CUSTOMER, true)
                    .value(UsersTable.PICTURE, ByteBuffer.wrap(new byte[] { 4, 5, 5}))
                    .value(UsersTable.ADDRESSES, ImmutableList.of("münchen", "karlsruhe"))
                    .value(UsersTable.PHONE_NUMBERS, ImmutableSet.of("94665", "34324543"))
                    .ifNotExists()       
                    .execute();
            
            Assert.fail("DuplicateEntryException expected"); 
        } catch (ConstraintException expected) { }  

        
        
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
        optionalRecord.ifPresent(record -> System.out.println(record.getList(UsersTable.ADDRESSES, String.class).get()));
        System.out.println(optionalRecord.get());
        
        
        
        Optional<Record> optionalRecord2 = usersDao.readWithKey(UsersTable.USER_ID, "95454")
                                                   .columns(UsersTable.PICTURE, UsersTable.ADDRESSES, UsersTable.PHONE_NUMBERS)
                                                   .execute();
        Assert.assertTrue(optionalRecord2.isPresent());
        optionalRecord2.ifPresent(record -> System.out.println(record.getList(UsersTable.ADDRESSES, String.class).get()));

 
        
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
        Batchable insert1 = usersDao.writeWithKey(UsersTable.USER_ID, "14323425")
                                    .value(UsersTable.IS_CUSTOMER, true)
                                    .value(UsersTable.ADDRESSES, ImmutableList.of("berlin", "budapest"))
                                    .value(UsersTable.PHONE_NUMBERS, ImmutableSet.of("12313241243", "232323"));
        
        
        Batchable insert2 = usersDao.writeWithKey(UsersTable.USER_ID, "2222")
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
        Assert.assertEquals("8345345", record.getString(UsersTable.USER_ID).get());
        Assert.assertEquals(true, record.getBool(UsersTable.IS_CUSTOMER).get());
        
        Iterator<String> phoneNumbers = record.getSet(UsersTable.PHONE_NUMBERS, String.class).get().iterator();
        Assert.assertEquals("24234244", phoneNumbers.next());
        Assert.assertFalse(phoneNumbers.hasNext());
        
        
        
        // Inc/Set counter value 
        loginsDao.writeWithKey(LoginsTable.USER_ID, "8345345")
                .incr(LoginsTable.LOGINS)
                .execute();
        
        record = loginsDao.readWithKey(LoginsTable.USER_ID, "8345345")
                          .execute()
                          .get();
        Assert.assertEquals((Long) 1l, record.getLong(LoginsTable.LOGINS).get());
        
        
        loginsDao.writeWithKey(LoginsTable.USER_ID, "8345345")
                 .incr(LoginsTable.LOGINS, 4)   
                 .execute();

        record = loginsDao.readWithKey(LoginsTable.USER_ID, "8345345")
                          .execute()
                          .get();
        Assert.assertEquals((Long) 5l, record.getLong(LoginsTable.LOGINS).get());

        
        
        loginsDao.writeWithKey(LoginsTable.USER_ID, "8345345")
                 .decr(LoginsTable.LOGINS)   
                 .execute();

        record = loginsDao.readWithKey(LoginsTable.USER_ID, "8345345")
                          .execute()
                          .get();
        Assert.assertEquals((Long) 4l, record.getLong(LoginsTable.LOGINS).get());


        
        loginsDao.writeWithKey(LoginsTable.USER_ID, "8345345")
                 .decr(LoginsTable.LOGINS)
                 .withConsistency(ConsistencyLevel.LOCAL_QUORUM)
                 .withWritetime(Instant.now().toEpochMilli() * 1000)
                 .execute();

        record = loginsDao.readWithKey(LoginsTable.USER_ID, "8345345")
                          .execute()
                          .get();
        Assert.assertEquals((Long) 3l, record.getLong(LoginsTable.LOGINS).get());



        
        loginsDao.writeWhere(QueryBuilder.eq(LoginsTable.USER_ID, "8345345"))
                 .incr(LoginsTable.LOGINS)
                 .withWritetime(Instant.now().toEpochMilli() * 1000)
                 .withConsistency(ConsistencyLevel.LOCAL_QUORUM)
                 .execute();

        record = loginsDao.readWithKey(LoginsTable.USER_ID, "8345345")
                          .execute()
                          .get();
        Assert.assertEquals((Long) 4l, record.getLong(LoginsTable.LOGINS).get());


        
        plusLoginsDao.writeWhere(QueryBuilder.eq(PlusLoginsTable.USER_ID, "8345345"))
                     .incr(PlusLoginsTable.LOGINS)
                     .withWritetime(Instant.now().toEpochMilli() * 1000)
                     .withConsistency(ConsistencyLevel.LOCAL_QUORUM)
                     .execute();
        
        record = plusLoginsDao.readWithKey(PlusLoginsTable.USER_ID, "8345345")
                          .execute()
                          .get();
        Assert.assertEquals((Long) 1l, record.getLong(PlusLoginsTable.LOGINS).get());
        
        
        

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
        Assert.assertEquals((Long) 2l, record.getLong(PlusLoginsTable.LOGINS).get());

        record = loginsDao.readWithKey(LoginsTable.USER_ID, "8345345")
                .execute()
                .get();
        Assert.assertEquals((Long) 5l, record.getLong(LoginsTable.LOGINS).get());


        
        
        
        
        
        
        
        // remove value
        usersDao.writeWithKey(UsersTable.USER_ID, "8345345")
                .value(UsersTable.IS_CUSTOMER, null)
                .execute();
        
    
        record = usersDao.readWithKey(UsersTable.USER_ID, "8345345")
                         .execute()
                         .get();
        Assert.assertEquals("8345345", record.getString(UsersTable.USER_ID).get());
        Assert.assertFalse(record.getBool(UsersTable.IS_CUSTOMER).get());

        phoneNumbers = record.getSet(UsersTable.PHONE_NUMBERS, String.class).get().iterator();
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
        } catch (ConstraintException expected) {  }

        record = usersDao.readWithKey(UsersTable.USER_ID, "2222")
                .execute()
                .get();
        
        Iterator<String> addresses= record.getList(UsersTable.ADDRESSES, String.class).get().iterator();
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
        
        addresses= record.getList(UsersTable.ADDRESSES, String.class).get().iterator();
        Assert.assertEquals("nürnberg", addresses.next());
        Assert.assertFalse(addresses.hasNext());   
        
        
        
        
        
        usersDao.writeWhere(QueryBuilder.in(UsersTable.USER_ID, "2222"))
                .value(UsersTable.ADDRESSES, ImmutableList.of("berlin", "budapest"))
                .execute();
        
        record = usersDao.readWithKey(UsersTable.USER_ID, "2222")
                .execute()
                .get();
        
        addresses= record.getList(UsersTable.ADDRESSES, String.class).get().iterator();
        Assert.assertEquals("berlin", addresses.next());
        Assert.assertEquals("budapest", addresses.next());
        Assert.assertFalse(addresses.hasNext());        
        
        
        
        
        
        try {
            usersDao.deleteWithKey(UsersTable.USER_ID, "2222")
                    .onlyIf(QueryBuilder.eq(UsersTable.IS_CUSTOMER, false))
                    .execute();
            Assert.fail("IfConditionException expected");
        } catch (ConstraintException expected) { }

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
        Assert.assertEquals("8345345", record.getString(UsersTable.USER_ID).get());
        Assert.assertTrue(record.getBool(UsersTable.IS_CUSTOMER).get());
        Assert.assertFalse(record.getList(UsersTable.ADDRESSES, String.class).get().isEmpty());
        Assert.assertFalse(record.getSet(UsersTable.PHONE_NUMBERS, String.class).get().isEmpty());
        


        
        
        
        // remove value
        usersDao.writeWithKey(UsersTable.USER_ID, "8345345")
                .value(UsersTable.IS_CUSTOMER, null)
                .value(UsersTable.ADDRESSES, null)
                .execute();        
        
        
        record = usersDao.readWithKey(UsersTable.USER_ID, "8345345")
                         .execute()
                         .get();
        Assert.assertEquals("8345345", record.getString(UsersTable.USER_ID).get());
        Assert.assertFalse(record.getBool(UsersTable.IS_CUSTOMER).get());
        Assert.assertFalse(record.getList(UsersTable.ADDRESSES, String.class).isPresent());

        
        
        // add set value
        usersDao.writeWithKey(UsersTable.USER_ID, "8345345")
                .addSetValue(UsersTable.PHONE_NUMBERS, "12142343")
                .execute();        
        
        
        record = usersDao.readWithKey(UsersTable.USER_ID, "8345345")
                         .execute()
                         .get();
        Assert.assertEquals("8345345", record.getString(UsersTable.USER_ID).get());
        Assert.assertFalse(record.getBool(UsersTable.IS_CUSTOMER).get());
        Assert.assertTrue(record.getSet(UsersTable.PHONE_NUMBERS, String.class).get().contains("12142343"));
        
        
        
        
        // add set value
        usersDao.writeWithKey(UsersTable.USER_ID, "8345345")
                .addSetValue(UsersTable.PHONE_NUMBERS, "23234234")
                .execute();        
        
        
        record = usersDao.readWithKey(UsersTable.USER_ID, "8345345")
                         .execute()
                         .get();
        Assert.assertEquals("8345345", record.getString(UsersTable.USER_ID).get());
        Assert.assertFalse(record.getBool(UsersTable.IS_CUSTOMER).get());
        Assert.assertTrue(record.getSet(UsersTable.PHONE_NUMBERS, String.class).get().contains("12142343"));
        Assert.assertTrue(record.getSet(UsersTable.PHONE_NUMBERS, String.class).get().contains("23234234"));
        
        
        // remove set value
        usersDao.writeWithKey(UsersTable.USER_ID, "8345345")
                .removeSetValue(UsersTable.PHONE_NUMBERS, "12142343")
                .execute();        
        
        
        record = usersDao.readWithKey(UsersTable.USER_ID, "8345345")
                         .execute()
                         .get();
        Assert.assertEquals("8345345", record.getString(UsersTable.USER_ID).get());
        Assert.assertFalse(record.getBool(UsersTable.IS_CUSTOMER).get());
        Assert.assertFalse(record.getSet(UsersTable.PHONE_NUMBERS, String.class).get().contains("12142343"));
        Assert.assertTrue(record.getSet(UsersTable.PHONE_NUMBERS, String.class).get().contains("23234234"));
        Assert.assertFalse(record.getList(UsersTable.ADDRESSES, String.class).isPresent());
        
        
        
        
        // add list value
        usersDao.writeWithKey(UsersTable.USER_ID, "8345345")
                .value(UsersTable.ADDRESSES, ImmutableList.of("bonn", "stuttgart"))
                .execute();        
        
        
        record = usersDao.readWithKey(UsersTable.USER_ID, "8345345")
                         .execute()
                         .get();
        Assert.assertEquals("8345345", record.getString(UsersTable.USER_ID).get());
        Assert.assertFalse(record.getBool(UsersTable.IS_CUSTOMER).get());
        Iterator<String> addrIt = record.getList(UsersTable.ADDRESSES, String.class).get().iterator();
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
        Assert.assertEquals("8345345", record.getString(UsersTable.USER_ID).get());
        Assert.assertFalse(record.getBool(UsersTable.IS_CUSTOMER).get());
        addrIt = record.getList(UsersTable.ADDRESSES, String.class).get().iterator();
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
        Assert.assertEquals("8345345", record.getString(UsersTable.USER_ID).get());
        Assert.assertEquals("xe333", record.getMap(UsersTable.ROLES, String.class, String.class).get().get("customer"));
        
        
        
        
        usersDao.writeWithKey(UsersTable.USER_ID, "8345345")
                .putMapValue(UsersTable.ROLES, "player_type1", "p14334")
                .putMapValue(UsersTable.ROLES, "player_type2", "p233")
                .execute();        
        
        record = usersDao.readWithKey(UsersTable.USER_ID, "8345345")
                         .execute()
                         .get();
        Assert.assertEquals("xe333", record.getMap(UsersTable.ROLES, String.class, String.class).get().get("customer"));
        Assert.assertEquals("p14334", record.getMap(UsersTable.ROLES, String.class, String.class).get().get("player_type1"));
        Assert.assertEquals("p233", record.getMap(UsersTable.ROLES, String.class, String.class).get().get("player_type2"));


        
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
                .withEnableTracking()
                .execute();

        record = usersDao.readWithKey(UsersTableFields.USER_ID, "343434")
                .execute()
                .get();
        
        Assert.assertEquals("343434", record.getValue(UsersTableFields.USER_ID).get());
        Assert.assertTrue(record.getValue(UsersTableFields.IS_CUSTOMER).isPresent());
        Assert.assertFalse(record.getValue(UsersTableFields.PHONE_NUMBERS).get().contains("12142343"));
        Assert.assertTrue(record.getValue(UsersTableFields.PHONE_NUMBERS).get().contains("34324543"));
        Assert.assertTrue(record.getValue(UsersTableFields.ADDRESSES).get().contains("karlsruhe"));
        Assert.assertEquals("xe333", record.getValue(UsersTableFields.ROLES).get().get("customer"));
        Assert.assertArrayEquals(new byte[] { 4, 5, 5}, record.getValue(UsersTableFields.PICTURE).get());
        
        
        
        
        
   
        
        usersDao.writeWithKey(UsersTableFields.USER_ID, "45436")
                .value(UsersTableFields.PHONE_NUMBERS, ImmutableSet.of("24234244"))
                .value(UsersTableFields.IS_CUSTOMER, true)
                .value(UsersTableFields.USER_TYPE, UserType.METAL)
                .execute();

        Assert.assertNotNull(info.getQueryTrace());
        
        
        
        record = usersDao.readWithKey(UsersTableFields.USER_ID, "45436")
                .execute()
                .get();
        
        Assert.assertEquals(UserType.METAL, record.getValue(UsersTableFields.USER_TYPE).get());
        
      }        
}


