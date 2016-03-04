package com.stacksync.syncservice.test.handler;

import java.sql.Connection;
import java.util.UUID;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.stacksync.commons.models.Device;
import com.stacksync.commons.models.User;
import com.stacksync.commons.models.Workspace;
import com.stacksync.syncservice.db.ConnectionPool;
import com.stacksync.syncservice.db.ConnectionPoolFactory;
import com.stacksync.syncservice.db.DAOFactory;
import com.stacksync.syncservice.db.DAOPersistenceContext;
import com.stacksync.syncservice.db.DeviceDAO;
import com.stacksync.syncservice.db.UserDAO;
import com.stacksync.syncservice.db.WorkspaceDAO;
import com.stacksync.syncservice.exceptions.dao.DAOException;
import com.stacksync.syncservice.rpc.parser.IParser;
import com.stacksync.syncservice.rpc.parser.JSONParser;
import com.stacksync.syncservice.test.benchmark.db.DatabaseHelper;
import com.stacksync.syncservice.util.Config;

public class BasicHandlerTest {


	private static IParser reader;
	private static WorkspaceDAO workspaceDAO;
	private static UserDAO userDao;
	private static DeviceDAO deviceDao;
	private static UUID user1 = UUID.randomUUID();
	private static ConnectionPool pool;
	@BeforeClass
	public static void initializeData() {

		try {

			Config.loadProperties();
			reader = new JSONParser();
			

			String datasource = Config.getDatasource();
			pool = ConnectionPoolFactory.getConnectionPool(datasource);
			
			DAOFactory factory = new DAOFactory(datasource);		
                        
			workspaceDAO = factory.getWorkspaceDao();
			userDao = factory.getUserDao();
			deviceDao = factory.getDeviceDAO();
			
                        DatabaseHelper db = new DatabaseHelper(pool);
                        
                        DAOPersistenceContext persistenceContext = db.beginTransaction();
                        
			User user = new User(user1, "tester1", "tester1", "AUTH_12312312", "a@a.a", 100L, 0L, 0L);
			userDao.add(user, persistenceContext);

			Workspace workspace = new Workspace(null, 1, user, false, false);
			workspaceDAO.add(workspace, persistenceContext);

			Device device = new Device(null, "junitdevice", user);
			deviceDao.add(device, persistenceContext);
                        
                        db.commitTransaction(persistenceContext);

		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	@Test
	public void doCorrectCommit() throws DAOException {

		String firstCommit = "{" + "'user':'111aaa'," + "'type':'commit'," + "'workspace':'junituser/',"
				+ "'requestId':'junitdevice-1361792472697'," + "'device':'junitdevice'," + "'metadata':[{"
				+ "'rootId':'stacksync'," + "'fileId':1," + "'version':1," + "'parentRootId':'',"
				+ "'parentFileId':''," + "'parentFileVersion':''," + "'updated':1362495000105," + "'status':'NEW',"
				+ "'lastModified':1362495000105," + "'checksum':3499525671," + "'clientName':'junitdevice',"
				+ "'fileSize':1968," + "'folder':'0'," + "'name':'pgadmin.log'," + "'path':'/',"
				+ "'mimetype':'text/plain'," + "'chunks':['a']}," + "{" + "'rootId':'stacksync'," + "'fileId':1,"
				+ "'version':2," + "'parentRootId':''," + "'parentFileId':''," + "'parentFileVersion':'',"
				+ "'updated':1362495999105," + "'status':'CHANGED'," + "'lastModified':1362495999105,"
				+ "'checksum':3499525600," + "'clientName':'junitdevice'," + "'fileSize':900," + "'folder':'0',"
				+ "'name':'pgadmin.log'," + "'path':'/'," + "'mimetype':'text/plain'," + "'chunks':['a','b']}," + "{"
				+ "'rootId':'stacksync'," + "'fileId':1," + "'version':3," + "'parentRootId':'',"
				+ "'parentFileId':''," + "'parentFileVersion':''," + "'updated':1362496999105," + "'status':'CHANGED',"
				+ "'lastModified':1362496999105," + "'checksum':3499525600," + "'clientName':'junitdevice',"
				+ "'fileSize':1900," + "'folder':'0'," + "'name':'pgadmin.log'," + "'path':'/',"
				+ "'mimetype':'text/plain'," + "'chunks':['b','c']}]}";

		byte[] firstCommitByte = firstCommit.getBytes();
		/*Commit commitFirst = (Commit) reader.readMessage(firstCommitByte);
		CommitResponse crm = handler.doCommit(commitFirst);

		int version = 1;
		List<RemoteCommitResponseObject> objects = crm.getObjects();
		for (RemoteCommitResponseObject object : objects) {
			// Check if all commits are true.
			assertTrue("Object committed.", object.isCommitted());
			// Check if commits are ordered
			assertTrue("Correct version.", version == object.getVersion());
			version++;
		}

		printResponse(crm);

		String secondVersion = "{" + "'user':'111aaa'," + "'type':'commit'," + "'workspace':'junituser/',"
				+ "'requestId':'junitdevice-1361792472697'," + "'device':'junitdevice'," + "'metadata':[{"
				+ "'rootId':'stacksync'," + "'fileId':1," + "'version':4," + "'parentRootId':'',"
				+ "'parentFileId':''," + "'parentFileVersion':''," + "'updated':1362497999105," + "'status':'CHANGED',"
				+ "'lastModified':1362497999105," + "'checksum':3499525600," + "'clientName':'junitdevice',"
				+ "'fileSize':2000," + "'folder':'0'," + "'name':'pgadmin.log'," + "'path':'/',"
				+ "'mimetype':'text/plain'," + "'chunks':['d','e']}]}";

		byte[] secondVersionByte = secondVersion.getBytes();
		Commit commitSecondVersion = (Commit) reader.readMessage(secondVersionByte);
		crm = handler.doCommit(commitSecondVersion);

		objects = crm.getObjects();
		RemoteCommitResponseObject object = objects.get(0);
		// Check if all commits are true.
		assertTrue("Object committed.", object.isCommitted());
		// Check if commits are ordered
		assertTrue("Correct version.", object.getMetadata().getVersion() == 4);
*/
	}

	@Test
	public void incorrectCommitNewObject() throws DAOException {

		String msg = "{" + "'user':'111aaa'," + "'type':'commit'," + "'workspace':'junituser/',"
				+ "'requestId':'junitdevice-1361792472697'," + "'device':'junitdevice'," + "'metadata':[{"
				+ "'rootId':'stacksync'," + "'fileId':2," + "'version':2," + "'parentRootId':'',"
				+ "'parentFileId':''," + "'parentFileVersion':''," + "'updated':1361792469000," + "'status':'NEW',"
				+ "'lastModified':1360830517000," + "'checksum':3499525600," + "'clientName':'junitdevice',"
				+ "'fileSize':1," + "'folder':'0'," + "'name':'pgadmin.log'," + "'path':'/',"
				+ "'mimetype':'text/plain'," + "'chunks':['a']}]}";

		/*Commit request = (Commit) reader.readMessage(msg.getBytes());
		CommitResponse crm = handler.doCommit(request);

		List<RemoteCommitResponseObject> objects = crm.getObjects();
		RemoteCommitResponseObject object = objects.get(0);
		// Check if all commits are true.
		assertFalse("Object committed.", object.isCommitted());
		// Check if commits are ordered
		assertTrue("Correct version.", object.getVersion() == 2);

		printResponse(crm);*/
	}

	@Test
	public void commitIncorrectVersion() throws DAOException {

		String firstCommit = "{" + "'user':'111aaa'," + "'type':'commit'," + "'workspace':'junituser/',"
				+ "'requestId':'junitdevice-1361792472697'," + "'device':'junitdevice'," + "'metadata':[{"
				+ "'rootId':'stacksync'," + "'fileId':3," + "'version':1," + "'parentRootId':'',"
				+ "'parentFileId':''," + "'parentFileVersion':''," + "'updated':1362495000105," + "'status':'NEW',"
				+ "'lastModified':1362495000105," + "'checksum':3499525671," + "'clientName':'junitdevice',"
				+ "'fileSize':1968," + "'folder':'0'," + "'name':'pgadmin.log'," + "'path':'/',"
				+ "'mimetype':'text/plain'," + "'chunks':['d']}," + "{" + "'rootId':'stacksync'," + "'fileId':3,"
				+ "'version':2," + "'parentRootId':''," + "'parentFileId':''," + "'parentFileVersion':'',"
				+ "'updated':1362495999105," + "'status':'CHANGED'," + "'lastModified':1362495999105,"
				+ "'checksum':3499525600," + "'clientName':'junitdevice'," + "'fileSize':900," + "'folder':'0',"
				+ "'name':'pgadmin.log'," + "'path':'/'," + "'mimetype':'text/plain'," + "'chunks':['e','c']}]}";

		byte[] firstCommitByte = firstCommit.getBytes();
		/*Commit commitFirst = (Commit) reader.readMessage(firstCommitByte);
		CommitResponse crm = handler.doCommit(commitFirst);

		int version = 1;
		List<RemoteCommitResponseObject> objects = crm.getObjects();
		for (RemoteCommitResponseObject object : objects) {
			// Check if all commits are true.
			assertTrue("Object committed.", object.isCommitted());
			// Check if commits are ordered
			assertTrue("Correct version.", version == object.getVersion());
			version++;
		}
		ObjectMetadata secondVersion = objects.get(1).getMetadata();

		printResponse(crm);

		String msg = "{" + "'user':'111aaa'," + "'type':'commit'," + "'workspace':'junituser/',"
				+ "'requestId':'junitdevice-1361792472697'," + "'device':'junitdevice'," + "'metadata':[{"
				+ "'rootId':'stacksync'," + "'fileId':3," + "'version':1," + "'parentRootId':'',"
				+ "'parentFileId':''," + "'parentFileVersion':''," + "'updated':1362495000105," + "'status':'NEW',"
				+ "'lastModified':1362495000105," + "'checksum':3499525671," + "'clientName':'junitdevice',"
				+ "'fileSize':1968," + "'folder':'0'," + "'name':'pgadmin.log'," + "'path':'/',"
				+ "'mimetype':'text/plain'," + "'chunks':['d']}]}";

		byte[] secondVersionByte = msg.getBytes();
		Commit commitSecondVersion = (Commit) reader.readMessage(secondVersionByte);
		crm = handler.doCommit(commitSecondVersion);

		objects = crm.getObjects();
		RemoteCommitResponseObject object = objects.get(0);
		// Check if committed is true
		assertTrue("Object committed.", object.isCommitted());
		// Check if commits are ordered
		assertTrue("Correct version.", object.getVersion() == 1);

		// Check metadata to be the last version
		ObjectMetadata metadata = object.getMetadata();
		assertTrue("Comparing metadata.", metadata.equals(secondVersion));
		*/
	}

	@Test
	public void commitWithNewDevice() throws DAOException {

		String msg = "{" + "'user':'111aaa'," + "'type':'commit'," + "'workspace':'junituser/',"
				+ "'requestId':'junitdevice-1361792472697'," + "'device':'junitdevice2'," + "'metadata':[{"
				+ "'rootId':'stacksync'," + "'fileId':4," + "'version':1," + "'parentRootId':'',"
				+ "'parentFileId':''," + "'parentFileVersion':''," + "'updated':1361792469000," + "'status':'NEW',"
				+ "'lastModified':1360830517000," + "'checksum':3499525600," + "'clientName':'junitdevice4',"
				+ "'fileSize':1," + "'folder':'0'," + "'name':'pgadmin.log'," + "'path':'/',"
				+ "'mimetype':'text/plain'," + "'chunks':['a']}]}";

		/*Commit request = (Commit) reader.readMessage(msg.getBytes());
		CommitResponse crm = handler.doCommit(request);

		List<RemoteCommitResponseObject> objects = crm.getObjects();
		RemoteCommitResponseObject object = objects.get(0);
		// Check if all commits are true.
		assertTrue("Object committed.", object.isCommitted());
		// Check if commits are ordered
		assertTrue("Correct version.", object.getVersion() == 1);

		printResponse(crm);
*/
	}

	@AfterClass
	public static void cleanDB() throws DAOException, Exception {
            
                DatabaseHelper db = new DatabaseHelper(pool);
                        
                DAOPersistenceContext persistenceContext = db.beginTransaction();
                        
		userDao.delete(user1, persistenceContext);
                
                db.commitTransaction(persistenceContext);
	}

	/*private void printResponse(CommitResponse crm) {
		String response = reader.createResponse(crm);
		System.out.println(response);
	}*/

}
