/**
 * 
 */
package net.oneandone.troilus.api;

import java.io.IOException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Date;
import java.util.Iterator;

import junit.framework.TestCase;
import net.oneandone.troilus.CassandraDB;
import net.oneandone.troilus.Count;
import net.oneandone.troilus.Dao;
import net.oneandone.troilus.DaoImpl;
import net.oneandone.troilus.Field;
import net.oneandone.troilus.ListRead;
import net.oneandone.troilus.ListReadWithUnit;
import net.oneandone.troilus.Record;
import net.oneandone.troilus.ResultList;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.BlockJUnit4ClassRunner;

import com.datastax.driver.core.PagingState;
import com.datastax.driver.core.Session;

/**
 * Checks pagination API, verifying paging works and sorts
 * records properly
 * 
 * Pagination requires:
 * - fetchSize  (size of each page)
 * - pagingState
 * 
 * @author Jason Westra
 *
 */
@RunWith(value=BlockJUnit4ClassRunner.class)
public class PaginationTest extends TestCase implements PaginationInvites {

	private static CassandraDB cassandra;
	 
	public static final String keyspace = "ks_"+System.currentTimeMillis();
	
	public static final String TABLE_NAME = "invites_by_group";
	
	private static int ROW_COUNT = 100;
	
		
	@BeforeClass
    public static void beforeClass() throws IOException {
        cassandra = CassandraDB.newInstance();
        dropKeyspace();
		createKeyspace();
		
		cassandra.tryExecuteCqlFile(PaginationInvites.DDL);
        loadInvites();
	}
        
    @AfterClass
    public static void afterClass() throws IOException {
    	dropKeyspace();
        cassandra.close();
    }
	
    private static void createKeyspace() {
    	cassandra.getSession().execute("CREATE KEYSPACE "+keyspace+" with replication={'class': 'SimpleStrategy', 'replication_factor' : 1};");
		cassandra.executeCql("USE "+keyspace+";");
	}

	private static void dropKeyspace() {
		try {
			Session session = cassandra.getSession();
			session.execute("DROP KEYSPACE "+keyspace+";");
		} catch(Exception e) {}
	}
	
	// Loads 100 invites, they should be .x seconds apart and an ordered fetch
	// should being back the rows in order
	private static void loadInvites() {
		for (int i = 1; i <= ROW_COUNT; i++) {
			cassandra.executeCql(
					"INSERT INTO "+TABLE_NAME+
					" (group_id, invite_date, email_address)"+
					" VALUES ('group_1', toTimestamp(NOW()), 'a"+i+"@foo.com');");
			try {
				Thread.sleep(100);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}
	
	@Test
	public void testLoadInvites() {
		Dao dao = new DaoImpl(cassandra.getSession(), TABLE_NAME);
		ListReadWithUnit<ResultList<Record>, Record> listReadUnit = dao.readSequenceWithKey("group_id", "group_1");
		Count count = listReadUnit.count().execute();
		
		assertEquals(ROW_COUNT, count.getCount());
	}
	
	
	@Test
	public void testFetchInvitesAsList() {
		Dao dao = new DaoImpl(cassandra.getSession(), TABLE_NAME);
		ListReadWithUnit<ResultList<Record>, Record> listReadUnit = dao.readSequenceWithKey("group_id", "group_1");
		ResultList<Record> resultList = listReadUnit.all().execute();
		
		Iterator<Record> i = resultList.iterator();
		int numRecords = assertSortOrder(i);
		assertEquals("Size should be "+ROW_COUNT, ROW_COUNT, numRecords);
	}
	
	// Limit doesn't really do pagination,but this is a sanity check
	// to see that limit is not broken by any pagination API changes
	// and data comes back sorted as expectd
	@Test
	public void testFetchInvitesListWithLimit() {
		int LIMIT = 30;
		Dao dao = new DaoImpl(cassandra.getSession(), TABLE_NAME);
		ListReadWithUnit<ResultList<Record>, Record> listReadUnit = dao.readSequenceWithKey("group_id", "group_1");
		ListRead<ResultList<Record>, Record> listread = listReadUnit.all();
		ResultList<Record> resultList = listread.withLimit(LIMIT).execute();
		
		Iterator<Record> i = resultList.iterator();
		int numRecords = assertSortOrder(i);
		
		assertEquals("Fetched incorrect number of records", LIMIT, numRecords);
	}
	
	@Test
	public void testFetchInvitesPageOfRecords() {
		PagingState pagingState = null;
				
		// page #, page size, # of expected results in the page
		pagingState = fetchAndAssert(1, 30, 30, pagingState);
		pagingState = fetchAndAssert(2, 30, 30, pagingState);
		pagingState = fetchAndAssert(3, 30, 30, pagingState);
		pagingState = fetchAndAssert(4, 30, 10, pagingState);
		
		// Last page results in empty paging state again
		assertNull(pagingState);
	}
	
	@Test
	public void testFetchInvitesPageOfEntities() {
		PagingState pagingState = null;
				
		// page #, page size, # of expected results in the page
		pagingState = fetchEntityAndAssert(1, 30, 30, pagingState);
		pagingState = fetchEntityAndAssert(2, 30, 30, pagingState);
		pagingState = fetchEntityAndAssert(3, 30, 30, pagingState);
		pagingState = fetchEntityAndAssert(4, 30, 10, pagingState);
		
		// Last page results in empty paging state again
		assertNull(pagingState);
	}
	
	
	private PagingState fetchAndAssert(int pageNumber, int pageSize, int expectedSize, PagingState pagingState) {
		Dao dao = new DaoImpl(cassandra.getSession(), TABLE_NAME);
		
		ListReadWithUnit<ResultList<Record>, Record> listReadUnit = dao.readSequenceWithKey("group_id", "group_1");
				
		// Pagination requires both: fetchSize and pagingState
		ListRead<ResultList<Record>, Record> listRead = listReadUnit.all()
				.withFetchSize(pageSize)
				.withPagingState(pagingState);
		
		ResultList<Record> resultList = listRead.execute();
		
		Iterator<Record> i = resultList.iterator();
			
		int numRecords = assertSortOrder(i);
		
		assertEquals("Size should be "+expectedSize, expectedSize, numRecords);
		
		return resultList.getExecutionInfo().getPagingState();
	}
	
	private PagingState fetchEntityAndAssert(int pageNumber, int pageSize, int expectedSize, PagingState pagingState) {
		ResultList<InvitesByMonthAndInviteDate> resultList = 
				new DaoImpl(cassandra.getSession(), TABLE_NAME)
			.readSequenceWithKey("group_id", "group_1")
			.asEntity(InvitesByMonthAndInviteDate.class)
			.withFetchSize(pageSize)
			.withPagingState(pagingState)
			.execute();
			
		int numRecords = assertSortOrder(resultList);
		
		assertEquals("Size should be "+expectedSize, expectedSize, numRecords);
		
		return resultList.getExecutionInfo().getPagingState();
	}
	
	/**
	 * @param i
	 * @return number of rows iterated over
	 */
	private int assertSortOrder(Iterator<Record> i) {
		LocalDateTime previousInviteDate = null;
		int cnt = 0;
		while(i.hasNext()) {
			Record record = i.next();
			LocalDateTime inviteDate = convert(record.getValue(INVITE_DATE, Date.class));
			if (previousInviteDate != null) {
				if (previousInviteDate.isAfter(inviteDate)) {
					fail("Fetched out of order of the invite date");
				}
			}
			
			previousInviteDate = inviteDate;
			cnt++;
		}
		return cnt;
	}
	
	/**
	 * @param results
	 * @return number of rows iterated over
	 */
	private int assertSortOrder(ResultList<InvitesByMonthAndInviteDate> results) {
		Iterator<InvitesByMonthAndInviteDate> i = results.iterator();
		LocalDateTime previousInviteDate = null;
		int cnt = 0;
		while(i.hasNext()) {
			InvitesByMonthAndInviteDate invite = i.next();
			
			LocalDateTime inviteDate = convert(invite.getInviteDate());
			if (previousInviteDate != null) {
				if (previousInviteDate.isAfter(inviteDate)) {
					fail("Fetched out of order of the invite date");
				}
			}
			
			previousInviteDate = inviteDate;
			cnt++;
		}
		return cnt;
	}
	
	public static class InvitesByMonthAndInviteDate {
		
		
		@Field(name="group_id")
		private String groupId;
		
		@Field(name="email_address")
		private String emailAddress;
		
		@Field(name="invite_date")
		private Date inviteDate;


		/**
		 * @return the groupId
		 */
		public String getGroupId() {
			return groupId;
		}

		/**
		 * @param groupId the groupId to set
		 */
		public void setGroupId(String groupId) {
			this.groupId = groupId;
		}
		
		/**
		 * @return the emailAddress
		 */
		public String getEmailAddress() {
			return emailAddress;
		}

		/**
		 * @param emailAddress the emailAddress to set
		 */
		public void setEmailAddress(String emailAddress) {
			this.emailAddress = emailAddress;
		}

		/**
		 * @return the inviteDate
		 */
		public Date getInviteDate() {
			return inviteDate;
		}

		/**
		 * @param inviteDate the inviteDate to set
		 */
		public void setInviteDate(Date inviteDate) {
			this.inviteDate = inviteDate;
		}
		
	}
	
	
	
	private LocalDateTime convert(Date date) {
		Instant instant = Instant.ofEpochMilli(date.getTime());
		LocalDateTime ldt = LocalDateTime.ofInstant(instant, ZoneOffset.UTC);
		return ldt;
	}

}
