/**
 * 
 */
package net.oneandone.troilus.userdefinieddatatypes;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import junit.framework.TestCase;
import net.oneandone.troilus.CassandraDB;
import net.oneandone.troilus.ColumnName;
import net.oneandone.troilus.Dao;
import net.oneandone.troilus.DaoImpl;
import net.oneandone.troilus.Deletion;
import net.oneandone.troilus.Field;
import net.oneandone.troilus.Write;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.BlockJUnit4ClassRunner;

import com.datastax.driver.core.Session;



/**
 * Tests APIs against Single embedded UDT, List of UDT, Set of UDT and Map of UDT
 * 
 * Hotels example only tests collections with native DataTypes
 * UserDefinedDataTypesTest.java does not test updating and entity with Collections of UDTValue
 * 
 * @author Jason Westra 12-10-2015
 *
 */
@RunWith(value=BlockJUnit4ClassRunner.class)
public class UDTValueMappingCollectionTests extends TestCase {

	public static final ColumnName<Map<String, DescriptionUDT>> DESCRIPTIONS = ColumnName.defineMap("descriptions", String.class, DescriptionUDT.class);
	
	private static CassandraDB cassandra;
	 
	Session session;
	
	public static final String keyspace = "ks_"+System.currentTimeMillis();
	
	public static final String TABLE_MOCK_WITH_UDT_LIST = "mock_with_udt_list";
	public static final String TABLE_MOCK_WITH_UDT_SET = "mock_with_udt_set";
	public static final String TABLE_MOCK_WITH_UDT_MAP = "mock_with_udt_map";
	public static final String TABLE_MOCK_WITH_UDT = "mock_with_udt_single";
	public static final String TABLE_MOCK_WITH_MULTI_UDT_MAP = "mock_with_multi_udt_map";
	
	@BeforeClass
    public static void beforeClass() throws IOException {
        cassandra = CassandraDB.newInstance();
        
        dropKeyspace();
		createKeyspace();
		createUDTs();
		createTables();	
    }
        
    @AfterClass
    public static void afterClass() throws IOException {
    	dropKeyspace();
        cassandra.close();
    }
	    
	@Before
	public void setUp() throws Exception {
		session = cassandra.getSession();
		
	}
		
	private static void createKeyspace() {
		cassandra.getSession().execute("CREATE KEYSPACE "+keyspace+" with replication={'class': 'SimpleStrategy', 'replication_factor' : 1};");
	}

	private static void createTables() {
		Session session = cassandra.getSession();
		
		session.execute("CREATE TABLE "+keyspace+"."+TABLE_MOCK_WITH_UDT_LIST+" (id text, version bigint, create_date timestamp, descriptions list<frozen<description>>, PRIMARY KEY (id));");
		session.execute("CREATE TABLE "+keyspace+"."+TABLE_MOCK_WITH_UDT_SET+" (id text, version bigint, create_date timestamp, descriptions set<frozen<description>>, PRIMARY KEY (id));");
		session.execute("CREATE TABLE "+keyspace+"."+TABLE_MOCK_WITH_UDT_MAP+" (id text, version bigint, create_date timestamp, descriptions map<text,frozen<description>>, PRIMARY KEY (id));");
		session.execute("CREATE TABLE "+keyspace+"."+TABLE_MOCK_WITH_UDT+" (id text, version bigint, create_date timestamp, description frozen<description>, PRIMARY KEY (id));");
		session.execute("CREATE TABLE "+keyspace+"."+TABLE_MOCK_WITH_MULTI_UDT_MAP+" (id text, version bigint, create_date timestamp, descriptions map<text,frozen<description>>, other_descriptions map<text,frozen<description>>, PRIMARY KEY (id));");
	}
	
	
	private static void createUDTs() {
		Session session = cassandra.getSession();
		session.execute("CREATE TYPE "+keyspace+".description (name text, time timestamp)");
	}

	private static void dropKeyspace() {
		try {
			Session session = cassandra.getSession();
			session.execute("DROP KEYSPACE "+keyspace+";");
		} catch(Exception e) {
			
		}
	}
	
	@Test
	public void testEntityWithUDTSingle() throws Exception {
		MockDOWithUDTSingle dataObject = new MockDOWithUDTSingle();
		dataObject.setCreateDate(new Date());
		dataObject.setId(System.currentTimeMillis()+"");
		dataObject.setVersion(1);
		
		DescriptionUDT description = new DescriptionUDT();
		description.name = "someName";
		description.time = new Date();
		
		dataObject.setDescription(description);
		
		Dao dao = new DaoImpl(session, keyspace, TABLE_MOCK_WITH_UDT_LIST);
		dao.writeEntity(dataObject)
			.ifNotExists()
			.execute();
		
		MockDOWithUDTSingle entityAsInserted = null;
		try {
			entityAsInserted = dao.readWithKey("id", dataObject.getId())
					.asEntity(MockDOWithUDTSingle.class)
					.execute().get();
		} catch(Exception e) {
			e.printStackTrace();
			// if will fail on assert below
		}
		
		assertNotNull(entityAsInserted);
	}
		
	@Test
	public void testEntityWithUDTList() throws Exception {
		MockDOWithUDTList dataObject = new MockDOWithUDTList();
		dataObject.setCreateDate(new Date());
		dataObject.setId(System.currentTimeMillis()+"");
		dataObject.setVersion(1);
		
		DescriptionUDT description1 = new DescriptionUDT();
		description1.name = "someName1";
		description1.time = new Date();
		
		DescriptionUDT description2 = new DescriptionUDT();
		description2.name = "someName2";
		description2.time = new Date();
				
		ArrayList<DescriptionUDT> descriptions = new ArrayList<DescriptionUDT>(); 
		descriptions.add(description1);
		descriptions.add(description2);
		
		dataObject.setDescriptions(descriptions);
		
		Dao dao = new DaoImpl(session, keyspace, TABLE_MOCK_WITH_UDT_LIST);
		dao.writeEntity(dataObject)
			.ifNotExists()
			.execute();
		
		MockDOWithUDTList entityAsInserted = null;
		try {
			entityAsInserted = dao.readWithKey("id", dataObject.getId())
					.asEntity(MockDOWithUDTList.class)
					.execute().get();
		} catch(Exception e) {
			e.printStackTrace();
			throw e;
		}
		
		assertNotNull(entityAsInserted);
		assertTrue(entityAsInserted.getDescriptions().size() == 2);
		
	}
	
	@Test
	public void testEntityWithUDTSet() throws Exception {
		MockDOWithUDTSet dataObject = new MockDOWithUDTSet();
		dataObject.setCreateDate(new Date());
		dataObject.setId(System.currentTimeMillis()+"");
		dataObject.setVersion(1);
		
		DescriptionUDT description1 = new DescriptionUDT();
		description1.name = "someName1";
		description1.time = new Date();
		
		DescriptionUDT description2 = new DescriptionUDT();
		description2.name = "someName2";
		description2.time = new Date();
				
		HashSet<DescriptionUDT> descriptions = new HashSet<DescriptionUDT>(); 
		descriptions.add(description1);
		descriptions.add(description2);
		
		dataObject.setDescriptions(descriptions);
		
		Dao dao = new DaoImpl(session, keyspace, TABLE_MOCK_WITH_UDT_SET);
		dao.writeEntity(dataObject)
			.ifNotExists()
			.execute();
		
		MockDOWithUDTSet entityAsInserted = null;
		try {
			entityAsInserted = dao.readWithKey("id", dataObject.getId())
					.asEntity(MockDOWithUDTSet.class)
					.execute().get();
		} catch(Exception e) {
			e.printStackTrace();
			throw e;
		}
		
		assertNotNull(entityAsInserted);
		assertTrue(entityAsInserted.getDescriptions().size() == 2);
		
	}
	
	 @Test
	 public void testEntityWithUDTMap() throws Exception {
		 MockDOWithUDTMap dataObject = new MockDOWithUDTMap();
		 dataObject.setCreateDate(new Date());
		 dataObject.setId(System.currentTimeMillis()+"");
		 dataObject.setVersion(1);
		 
		 DescriptionUDT description1 = new DescriptionUDT();
		 description1.name = "someName1";
		 description1.time = new Date();
		 
		 DescriptionUDT description2 = new DescriptionUDT();
		 description2.name = "someName2";
		 description2.time = new Date();
		 		
		 Map<String, DescriptionUDT> descriptions = new HashMap<String, DescriptionUDT>(); 
		 descriptions.put("1", description1);
		 descriptions.put("2", description2);
		
		 dataObject.setDescriptions(descriptions);
		
		 Dao dao = new DaoImpl(session, keyspace, TABLE_MOCK_WITH_UDT_MAP);
		 dao.writeEntity(dataObject)
		 	.ifNotExists()
		 	.execute();
		
		 MockDOWithUDTMap entityAsInserted = null;
		 try {
		 	entityAsInserted = dao.readWithKey("id", dataObject.getId())
		 			.asEntity(MockDOWithUDTMap.class)
		 			.execute().get();
		 } catch(Exception e) {
		 	e.printStackTrace();
		 	throw e;
		 }
		 
		 assertNotNull(entityAsInserted);
		 assertTrue(entityAsInserted.getDescriptions().size() == 2);
	 	
	 }
	
	 
	 /**
	  * this method tests that the change to add an entry into a map 
	  * succeeds
	  * 
	  * @throws Exception
	  */
	 @Test
	 public void testAddObjectsToMap() throws Exception {
		 
		 Dao dao = new DaoImpl(session, keyspace, TABLE_MOCK_WITH_UDT_MAP);
		 
		 
	     MockDOWithUDTMap dataObject = new MockDOWithUDTMap();
		 dataObject.setCreateDate(new Date());
		 dataObject.setId(System.currentTimeMillis()+"");
		 dataObject.setVersion(1);
		 
		 DescriptionUDT description1 = new DescriptionUDT();
		 description1.name = "someName1";
		 description1.time = new Date();
		 
		 DescriptionUDT description2 = new DescriptionUDT();
		 description2.name = "someName2";
		 description2.time = new Date();
		 
		 Map<String, DescriptionUDT> descriptions = new HashMap<String, DescriptionUDT>();
		 descriptions.put("1", description1);
		 descriptions.put("2", description2);
		
		 dataObject.setDescriptions(descriptions);
		
		 
		 //write entity
		 dao.writeEntity(dataObject)
		 .ifNotExists()
		 .execute();
		 
		 Map<String, DescriptionUDT> objectMap = new HashMap<String, DescriptionUDT>();
		 
		 DescriptionUDT description3 = new DescriptionUDT();
		 description3.name = "someName3";
		 description3.time = new Date();
		 
		 DescriptionUDT description4 = new DescriptionUDT();
		 description4.name = "someName4";
		 description4.time = new Date();

		 objectMap.put("3", description3);
		 objectMap.put("4", description4);
		 
		 Write update = dao
		 		 .writeWithKey("id", dataObject.getId());
		 
		 for(Entry<String, DescriptionUDT> entry : objectMap.entrySet()) {
			 update = update.putMapValue(DESCRIPTIONS, entry.getKey(), entry.getValue());
		 }

		 update.execute();
		
		 MockDOWithUDTMap entity = null;
		 
		 try {
			 entity = dao.readWithKey("id", dataObject.getId())
					 .asEntity(MockDOWithUDTMap.class)
					 .execute().get();
		 	
		 }catch(Exception e) {
			 e.printStackTrace();
			 throw e;
		 }
		
		 assertNotNull(entity);
		 assertTrue(entity.getDescriptions().size() ==4);
	 }
	 
	 /**
	  * this method tests that the change to add an entry into a map 
	  * succeeds when a null value slips in the cracks
	  * 
	  * @throws Exception
	  */
	 @Test
	 public void testAddObjectsIgnoreNullsToMap() throws Exception {
		 
		 Dao dao = new DaoImpl(session, keyspace, TABLE_MOCK_WITH_UDT_MAP);
		 
		 
	     MockDOWithUDTMap dataObject = new MockDOWithUDTMap();
		 dataObject.setCreateDate(new Date());
		 dataObject.setId(System.currentTimeMillis()+"");
		 dataObject.setVersion(1);
		 
		 DescriptionUDT description1 = new DescriptionUDT();
		 description1.name = "someName1";
		 description1.time = new Date();
		 
		 DescriptionUDT description2 = new DescriptionUDT();
		 description2.name = "someName2";
		 description2.time = new Date();
		 
		 Map<String, DescriptionUDT> descriptions = new HashMap<String, DescriptionUDT>();
		 descriptions.put("1", description1);
		 descriptions.put("2", description2);
		
		 dataObject.setDescriptions(descriptions);
		
		 
		 //write entity
		 dao.writeEntity(dataObject)
		 .ifNotExists()
		 .execute();
		 
		 Map<String, DescriptionUDT> objectMap = new HashMap<String, DescriptionUDT>();
		 
		 DescriptionUDT description3 = new DescriptionUDT();
		 description3.name = "someName3";
		 description3.time = new Date();
		 
		 DescriptionUDT description4 = new DescriptionUDT();
		 description4.name = "someName4";
		 description4.time = new Date();

		 objectMap.put("3", description3);
		 objectMap.put("4", null);
		 
		 Write update = dao
		 		 .writeWithKey("id", dataObject.getId());
		 
		 for(Entry<String, DescriptionUDT> entry : objectMap.entrySet()) {
			 update = update.putMapValue(DESCRIPTIONS, entry.getKey(), entry.getValue());
		 }

		 update.execute();
		
		 MockDOWithUDTMap entity = null;
		 
		 try {
			 entity = dao.readWithKey("id", dataObject.getId())
					 .asEntity(MockDOWithUDTMap.class)
					 .execute().get();
		 	
		 }catch(Exception e) {
			 e.printStackTrace();
			 throw e;
		 }
		
		 assertNotNull(entity);
		 assertTrue(entity.getDescriptions().size() ==3);
	 }
	 
	 
	 
	 /**
	  * this method tests that we can add to two separate maps in one call
	  * 
	  * @throws Exception
	  */
	 @Test
	 public void testAddObjectsToMultipleMapsToMap() throws Exception {
		 
		 Dao dao = new DaoImpl(session, keyspace, TABLE_MOCK_WITH_MULTI_UDT_MAP);
		 
		 
	     MockDOWithUDTMap dataObject = new MockDOWithUDTMap();
		 dataObject.setCreateDate(new Date());
		 dataObject.setId(System.currentTimeMillis()+"");
		 dataObject.setVersion(1);
		 
		 DescriptionUDT description1 = new DescriptionUDT();
		 description1.name = "someName1";
		 description1.time = new Date();
		 
		 DescriptionUDT description2 = new DescriptionUDT();
		 description2.name = "someName2";
		 description2.time = new Date();
		 
		 Map<String, DescriptionUDT> descriptions = new HashMap<String, DescriptionUDT>();
		 descriptions.put("1", description1);
		 descriptions.put("2", description2);
		
		 dataObject.setDescriptions(descriptions);
		
		 
		 //write entity
		 dao.writeEntity(dataObject)
		 .ifNotExists()
		 .execute();
		 
		 Map<String, DescriptionUDT> objectMap = new HashMap<String, DescriptionUDT>();
		 
		 DescriptionUDT description3 = new DescriptionUDT();
		 description3.name = "someName3";
		 description3.time = new Date();
		 
		 DescriptionUDT description4 = new DescriptionUDT();
		 description4.name = "someName4";
		 description4.time = new Date();

		 objectMap.put("3", description3);
		 objectMap.put("4", description4);
		 
		 //objects for second map
		 DescriptionUDT description5 = new DescriptionUDT();
		 description5.name = "someName5";
		 description5.time = new Date();
		 
		 DescriptionUDT description6 = new DescriptionUDT();
		 description6.name = "someName6";
		 description6.time = new Date();
		 
		 Map<String, Object> objectMap2 = new HashMap<String, Object>();
		 
		 objectMap2.put("5", description5);
		 objectMap2.put("6", description6);

		 Write update = dao
		 		 .writeWithKey("id", dataObject.getId());
		 
		 for(Entry<String, DescriptionUDT> entry : objectMap.entrySet()) {
			 update = update.putMapValue("descriptions", entry.getKey(), entry.getValue());
		 }
		 
		 for(Entry<String, Object> entry : objectMap2.entrySet()) {
			 update = update.putMapValue("other_descriptions", entry.getKey(), entry.getValue());
		 }
		 update.execute();
		
		 MockDOWithUDTMap entity = null;
		 
		 try {
			 entity = dao.readWithKey("id", dataObject.getId())
					 .asEntity(MockDOWithUDTMap.class)
					 .execute().get();
		 	
		 }catch(Exception e) {
			 e.printStackTrace();
			 throw e;
		 }
		
		 assertNotNull(entity);
		 assertTrue(entity.getDescriptions().size() ==4);
		 assertTrue(entity.getOtherDescriptions().size() ==2);
	 }
	 

	 
	 
	 
	 /**
	  * this method tests whether or not an update to a map entry
	  * succeeds.  if you add a map update to an existing entry, the expectation
	  * is that it if you add the map entry with a key that already exists in the map
	  * that it will overwrite the existing entry value
	  * 
	  * @throws Exception
	  */
	 @Test
	 public void testUpdateObjectInMap() throws Exception {
		 
		 Dao dao = new DaoImpl(session, keyspace, TABLE_MOCK_WITH_UDT_MAP);
		 
		 MockDOWithUDTMap dataObject = new MockDOWithUDTMap();
		 dataObject.setCreateDate(new Date());
		 dataObject.setId(System.currentTimeMillis()+"");
		 dataObject.setVersion(1);
		 
		 DescriptionUDT description1 = new DescriptionUDT();
		 description1.name = "someName1";
		 description1.time = new Date();
		 
		 DescriptionUDT description2 = new DescriptionUDT();
		 description2.name = "someName2";
		 description2.time = new Date();
		 
		 Map<String, DescriptionUDT> descriptions = new HashMap<String, DescriptionUDT>();
		 descriptions.put("1", description1);
		 descriptions.put("2", description2);
		 
		 dataObject.setDescriptions(descriptions);

		 dao.writeEntity(dataObject)
		 .ifNotExists()
		 .execute();

		 Map<String, DescriptionUDT> objectMap = new HashMap<String, DescriptionUDT>();
		 
		 DescriptionUDT description3 = new DescriptionUDT();
		 description3.name = "someName3";
		 description3.time = new Date();
		 
		 DescriptionUDT description4 = new DescriptionUDT();
		 description4.name = "someName4";
		 description4.time = new Date();
		 
		 DescriptionUDT descriptionUpdate = new DescriptionUDT();
		 descriptionUpdate.name = "updatedDescription2";
		 descriptionUpdate.time = new Date();

		 objectMap.put("2", descriptionUpdate);
		 objectMap.put("3", description3);
		 objectMap.put("4", description4);
		 
		 Write update = dao
		 		 .writeWithKey("id", dataObject.getId());
		 
		 for(Entry<String, DescriptionUDT> entry : objectMap.entrySet()) {
			 update = update.putMapValue("descriptions", entry.getKey(), entry.getValue());
		 }

		 update.execute();
		 MockDOWithUDTMap entity = null;
		
		 try {

			 entity = dao.readWithKey("id", dataObject.getId())
					 .asEntity(MockDOWithUDTMap.class)
					 .execute().get();
			
			 DescriptionUDT desc = entity.getDescriptions().get("2");
			 assertTrue("update was unsuccessful, name was " + desc.getName(), "updatedDescription2".equals(desc.getName()));
		 }catch(Exception e) {
			 e.printStackTrace();
			 throw e;
		 }
		
		 assertNotNull(entity);
		 assertTrue(entity.getDescriptions().size() ==4);

	 }
	
	 /**
	  * this method tests whether the new removeMapValue functionality 
	  * succeeds
	  * 
	  * @throws Exception
	  */
	 @Test
	 public void testRemoveObjectsFromMap() throws Exception {
		 
		 Dao dao = new DaoImpl(session, keyspace, TABLE_MOCK_WITH_UDT_MAP);
		 
		 MockDOWithUDTMap dataObject = new MockDOWithUDTMap();
		 dataObject.setCreateDate(new Date());
		 dataObject.setId(System.currentTimeMillis()+"");
		 dataObject.setVersion(1);
		 
		 DescriptionUDT description1 = new DescriptionUDT();
		 description1.name = "someName1";
		 description1.time = new Date();
		
		 DescriptionUDT description2 = new DescriptionUDT();
		 description2.name = "someName2";
		 description2.time = new Date();
		
		 DescriptionUDT description3 = new DescriptionUDT();
		 description3.name = "someName3";
		 description3.time = new Date();
		
		 DescriptionUDT description4 = new DescriptionUDT();
		 description4.name = "someName4";
		 description4.time = new Date();
		
		 Map<String, DescriptionUDT> descriptions = new HashMap<String, DescriptionUDT>();
		 descriptions.put("1", description1);
		 descriptions.put("2", description2);
		 descriptions.put("3", description3);
		 descriptions.put("4", description4);
		
		 dataObject.setDescriptions(descriptions);

		 dao.writeEntity(dataObject)
		 .ifNotExists()
		 .execute();

		 List<Object> descriptionsToRemove = new ArrayList<Object>();
		 descriptionsToRemove.add("2");
		 descriptionsToRemove.add("4");

		 Deletion deletion = dao
				 .deleteWithKey("id", dataObject.getId());
		 
		 for(Object o : descriptionsToRemove) {
			 deletion = deletion.removeMapValue(DESCRIPTIONS, o);
		 }

		 deletion.execute();
		
		 MockDOWithUDTMap entity = null;
		
		 try {

			 entity = dao.readWithKey("id", dataObject.getId())
					 .asEntity(MockDOWithUDTMap.class)
					 .execute().get();
			
		 }catch(Exception e) {
			 e.printStackTrace();
			 throw e;
		 }
		
		 assertNotNull(entity);
		 assertTrue(entity.getDescriptions().size() ==2);
	 }
	
	 /**
	  * this method tests whether the new removeMapValue functionality 
	  * succeeds if a null slips in the cracks
	  * 
	  * @throws Exception
	  */
	 @Test
	 public void testRemoveObjectsWithNullFromMap() throws Exception {
		 
		 Dao dao = new DaoImpl(session, keyspace, TABLE_MOCK_WITH_UDT_MAP);
		 
		 MockDOWithUDTMap dataObject = new MockDOWithUDTMap();
		 dataObject.setCreateDate(new Date());
		 dataObject.setId(System.currentTimeMillis()+"");
		 dataObject.setVersion(1);
		 
		 DescriptionUDT description1 = new DescriptionUDT();
		 description1.name = "someName1";
		 description1.time = new Date();
		
		 DescriptionUDT description2 = new DescriptionUDT();
		 description2.name = "someName2";
		 description2.time = new Date();
		
		 DescriptionUDT description3 = new DescriptionUDT();
		 description3.name = "someName3";
		 description3.time = new Date();
		
		 DescriptionUDT description4 = new DescriptionUDT();
		 description4.name = "someName4";
		 description4.time = new Date();
		
		 Map<String, DescriptionUDT> descriptions = new HashMap<String, DescriptionUDT>();
		 descriptions.put("1", description1);
		 descriptions.put("2", description2);
		 descriptions.put("3", description3);
		 descriptions.put("4", description4);
		
		 dataObject.setDescriptions(descriptions);

		 dao.writeEntity(dataObject)
		 .ifNotExists()
		 .execute();

		 List<Object> descriptionsToRemove = new ArrayList<Object>();
		 descriptionsToRemove.add("2");
		 descriptionsToRemove.add(null);

		 Deletion deletion = dao
				 .deleteWithKey("id", dataObject.getId());
		 
		 for(Object o : descriptionsToRemove) {
			 deletion = deletion.removeMapValue("descriptions", o);
		 }

		 deletion.execute();
		
		 MockDOWithUDTMap entity = null;
		
		 try {

			 entity = dao.readWithKey("id", dataObject.getId())
					 .asEntity(MockDOWithUDTMap.class)
					 .execute().get();
			
		 }catch(Exception e) {
			 e.printStackTrace();
			 throw e;
		 }
		
		 assertNotNull(entity);
		 assertTrue(entity.getDescriptions().size() ==3);
	 }
	 
	 /**
	  * Test iterating through a list of values to remove as well as adding 
	  * individual entries to remove
	  * 
	  * @throws Exception
	  */
	 @Test
	 public void testRemoveMapValuesMultipleCalls() throws Exception {
		 
		 Dao dao = new DaoImpl(session, keyspace, TABLE_MOCK_WITH_UDT_MAP);
		 
		 MockDOWithUDTMap dataObject = new MockDOWithUDTMap();
		 dataObject.setCreateDate(new Date());
		 dataObject.setId(System.currentTimeMillis()+"");
		 dataObject.setVersion(1);
		 
		 DescriptionUDT description1 = new DescriptionUDT();
		 description1.name = "someName1";
		 description1.time = new Date();
		
		 DescriptionUDT description2 = new DescriptionUDT();
		 description2.name = "someName2";
		 description2.time = new Date();
		
		 DescriptionUDT description3 = new DescriptionUDT();
		 description3.name = "someName3";
		 description3.time = new Date();
		
		 DescriptionUDT description4 = new DescriptionUDT();
		 description4.name = "someName4";
		 description4.time = new Date();
		
		 Map<String, DescriptionUDT> descriptions = new HashMap<String, DescriptionUDT>();
		 descriptions.put("1", description1);
		 descriptions.put("2", description2);
		 descriptions.put("3", description3);
		 descriptions.put("4", description4);
		
		 dataObject.setDescriptions(descriptions);

		 dao.writeEntity(dataObject)
		 .ifNotExists()
		 .execute();
		
		 List<Object> objectMap = new ArrayList<Object>();
		 objectMap.add("2");
		 
		 Deletion deletion = dao
				 .deleteWithKey("id", dataObject.getId());
		 
		 for(Object o : objectMap) {
			 deletion = deletion.removeMapValue("descriptions", o);
		 }
		 deletion = deletion.removeMapValue("descriptions","3");
		 deletion.execute();
		
		 MockDOWithUDTMap entity = null;
		
		 try {

			 entity = dao.readWithKey("id", dataObject.getId())
					 .asEntity(MockDOWithUDTMap.class)
					 .execute().get();
			
		 }catch(Exception e) {
			 e.printStackTrace();
			 throw e;
		 }
		
		 assertNotNull(entity);
		 assertTrue(entity.getDescriptions().size() ==2);
	 }
	 
	 @Test
	 public void testRemoveMultiplesCombinationFromMap() throws Exception {
		 MockDOWithUDTMap dataObject = new MockDOWithUDTMap();
		 dataObject.setCreateDate(new Date());
		 dataObject.setId(System.currentTimeMillis()+"");
		 dataObject.setVersion(1);
		 
		 DescriptionUDT description1 = new DescriptionUDT();
		 description1.name = "someName1";
		 description1.time = new Date();
		
		 DescriptionUDT description2 = new DescriptionUDT();
		 description2.name = "someName2";
		 description2.time = new Date();
		
		 DescriptionUDT description3 = new DescriptionUDT();
		 description3.name = "someName3";
		 description3.time = new Date();
		
		 DescriptionUDT description4 = new DescriptionUDT();
		 description4.name = "someName4";
		 description4.time = new Date();
		
		 Map<String, DescriptionUDT> descriptions = new HashMap<String, DescriptionUDT>();
		 descriptions.put("1", description1);
		 descriptions.put("2", description2);
		 descriptions.put("3", description3);
		 descriptions.put("4", description4);
		
		 dataObject.setDescriptions(descriptions);
		
		 Dao dao = new DaoImpl(session, keyspace, TABLE_MOCK_WITH_UDT_MAP);
		
		 dao.writeEntity(dataObject)
		 .ifNotExists()
		 .execute();

		
		 dao = new DaoImpl(session, keyspace, TABLE_MOCK_WITH_UDT_MAP);
		
		 
		 List<Object> objectMap = new ArrayList<Object>();
		 objectMap.add("2");
		 objectMap.add("3");
		 
		 Deletion deletion = dao
				 .deleteWithKey("id", dataObject.getId());
		 
		 for(Object o : objectMap) {
			 deletion = deletion.removeMapValue("descriptions", o);
		 }
		 deletion = deletion.removeMapValue("descriptions", "1");
		 
		 deletion.execute();
		
		 MockDOWithUDTMap entity = null;
		
		 try {
			 dao = new DaoImpl(session, keyspace, TABLE_MOCK_WITH_UDT_MAP);
			 entity = dao.readWithKey("id", dataObject.getId())
					 .asEntity(MockDOWithUDTMap.class)
					 .execute().get();
			
		 }catch(Exception e) {
			 e.printStackTrace();
			 throw e;
		 }
		
		 assertNotNull(entity);
		 assertTrue(entity.getDescriptions().size() ==1);
	 }
	 
	
	// Tests @Field shows up on subclasses
	abstract public static class AbstractDO {
		
		@Field(name="id")
		private String id;
				
		@Field(name="create_date")
		private Date createDate;
				
		@Field(name="version")
		private long version;

		/**
		 * @return the id
		 */
		public String getId() {
			return id;
		}

		/**
		 * @param id the id to set
		 */
		public void setId(String id) {
			this.id = id;
		}

		/**
		 * @return the createDate
		 */
		public Date getCreateDate() {
			return createDate;
		}

		/**
		 * @param createDate the createDate to set
		 */
		public void setCreateDate(Date createDate) {
			this.createDate = createDate;
		}

		/**
		 * @return the version
		 */
		public long getVersion() {
			return version;
		}

		/**
		 * @param version the version to set
		 */
		public void setVersion(long version) {
			this.version = version;
		}
	}
	
	public static class MockDOWithInheritance extends AbstractDO {
		
		@Field(name="latitude")
		private BigDecimal latitude;

		/**
		 * @return the latitude
		 */
		public BigDecimal getLatitude() {
			return latitude;
		}

		/**
		 * @param latitude the latitude to set
		 */
		public void setLatitude(BigDecimal latitude) {
			this.latitude = latitude;
		}
	}
	
	public static class MockDOWithUDTList extends AbstractDO {
		
		@Field(name="descriptions")
		private List<DescriptionUDT> descriptions;

		/**
		 * @return the descriptions
		 */
		public List<DescriptionUDT> getDescriptions() {
			return descriptions;
		}

		/**
		 * @param descriptions the descriptions to set
		 */
		public void setDescriptions(List<DescriptionUDT> descriptions) {
			this.descriptions = descriptions;
		}
	}
	
	public static class MockDOWithUDTSet extends AbstractDO {
		
		@Field(name="descriptions")
		private Set<DescriptionUDT> descriptions;

		/**
		 * @return the descriptions
		 */
		public Set<DescriptionUDT> getDescriptions() {
			return descriptions;
		}

		/**
		 * @param descriptions the descriptions to set
		 */
		public void setDescriptions(Set<DescriptionUDT> descriptions) {
			this.descriptions = descriptions;
		}
	}
	
	public static class MockDOWithUDTMap extends AbstractDO {
		
		@Field(name="descriptions")
		private Map<String, DescriptionUDT> descriptions;
		
		@Field(name="other_descriptions")
		private Map<String, DescriptionUDT> otherDescriptions;

		
		
		public Map<String, DescriptionUDT> getOtherDescriptions() {
			return otherDescriptions;
		}

		public void setOtherDescriptions(Map<String, DescriptionUDT> otherDescriptions) {
			this.otherDescriptions = otherDescriptions;
		}

		/**
		 * @return the descriptions
		 */
		public Map<String, DescriptionUDT> getDescriptions() {
			return descriptions;
		}

		/**
		 * @param descriptions the descriptions to set
		 */
		public void setDescriptions(Map<String, DescriptionUDT> descriptions) {
			this.descriptions = descriptions;
		}
	}
	
	public static class MockDOWithUDTSingle extends AbstractDO {
		
		@Field(name="description")
		DescriptionUDT description;

		/**
		 * @return the description
		 */
		public DescriptionUDT getDescription() {
			return description;
		}

		/**
		 * @param description the description to set
		 */
		public void setDescription(DescriptionUDT description) {
			this.description = description;
		}
	}
	
	
	// The User Defined Type
	public static class DescriptionUDT {
		
		@Field(name="name")
		private String name;
		
		@Field(name="time")
		private Date time;

		/**
		 * @return the name
		 */
		public String getName() {
			return name;
		}

		/**
		 * @param name the name to set
		 */
		public void setName(String name) {
			if (name == null) throw new IllegalArgumentException("name cannot be empty");
			this.name = name;
		}

		/**
		 * @return the time
		 */
		public Date getTime() {
			return time;
		}

		/**
		 * @param time the time to set
		 */
		public void setTime(Date time) {
			if (time == null) throw new IllegalArgumentException("time cannot be empty");
			this.time = time;
		}
		
		//////////////////////////////////////////////
		// REQUIRED FOR UDT THAT IS IN A SET
		// SMART TO HAVE REGARDLESS....
		//////////////////////////////////////////////
		public int hashCode() {
			return (this.time.hashCode() * 37) + (this.name.hashCode() * 37);
		}
		
		public boolean equals(Object o) {
			if (o instanceof DescriptionUDT) {
				DescriptionUDT other = (DescriptionUDT)o;
				if (other.time.equals(this.time) &&
						other.name.equals(this.name)) {
					return true;
				}
			}
			return false;
		}
	}
}
