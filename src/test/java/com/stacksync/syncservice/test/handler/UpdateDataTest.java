package com.stacksync.syncservice.test.handler;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.UUID;

import org.junit.BeforeClass;
import org.junit.Test;

import com.stacksync.commons.models.ItemMetadata;
import com.stacksync.commons.models.User;
import com.stacksync.commons.models.Workspace;
import com.stacksync.syncservice.db.ConnectionPool;
import com.stacksync.syncservice.db.ConnectionPoolFactory;
import com.stacksync.syncservice.db.DAOFactory;
import com.stacksync.syncservice.db.UserDAO;
import com.stacksync.syncservice.db.WorkspaceDAO;
import com.stacksync.syncservice.handler.APIHandler;
import com.stacksync.syncservice.handler.SQLAPIHandler;
import com.stacksync.syncservice.rpc.messages.APICommitResponse;
import com.stacksync.syncservice.util.Config;

public class UpdateDataTest {

	private static APIHandler handler;
	private static WorkspaceDAO workspaceDAO;
	private static UserDAO userDao;
	private static User user1;
	private static User user2;

	@BeforeClass
	public static void initializeData() throws Exception {

		Config.loadProperties();

		String datasource = Config.getDatasource();
		ConnectionPool pool = ConnectionPoolFactory.getConnectionPool(datasource);

		handler = new SQLAPIHandler(pool);
		DAOFactory factory = new DAOFactory(datasource);

		workspaceDAO = factory.getWorkspaceDao();
		userDao = factory.getUserDao();

		user1 = new User(UUID.fromString("225130d4-c817-4df0-b4e2-13271b494ae5"), "tester_2", "tester_2", "AUTH_5e446d39e4294b57831da7ce3dd0d2c2", "test@stacksync.org", 100000L,0L, 0L);
		
		/*
		userDao.add(user1);
		Workspace workspace1 = new Workspace(null, 1, user1, false, false);
		workspaceDAO.add(workspace1);

		user2 = new User(UUID.randomUUID(), "tester1", "tester1", "AUTH_12312312", "a@a.a", 100, 0);

		userDao.add(user2);
		Workspace workspace2 = new Workspace(null, 1, user2, false, false);
		workspaceDAO.add(workspace2);
		*/
	}
	
	@Test
	public void registerNewDevice() throws Exception {
		
		/*ItemMetadata file = new ItemMetadata();
		file.setId(509L);
		file.setMimetype("image/jpeg");
		file.setChecksum(0000000000L);
		file.setSize(900L);
		file.setChunks(Arrays.asList("11111", "22222", "333333"));
		
		APICommitResponse response = handler.updateData(user1, file);
		System.out.println(response.toString());*/
	}

}
