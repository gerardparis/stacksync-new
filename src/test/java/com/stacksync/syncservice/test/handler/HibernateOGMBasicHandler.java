package com.stacksync.syncservice.test.handler;

import java.util.UUID;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.stacksync.commons.models.Device;
import com.stacksync.commons.models.ItemMetadata;
import com.stacksync.commons.models.User;
import com.stacksync.commons.models.Workspace;
import com.stacksync.commons.notifications.CommitNotification;
import com.stacksync.syncservice.db.DAOPersistenceContext;
import com.stacksync.syncservice.db.DeviceDAO;
import com.stacksync.syncservice.db.ItemDAO;
import com.stacksync.syncservice.db.ItemVersionDAO;
import com.stacksync.syncservice.db.UserDAO;
import com.stacksync.syncservice.db.WorkspaceDAO;
import com.stacksync.syncservice.db.hibernateOGM.HibernateOGMEntityManagerFactory;
import com.stacksync.syncservice.db.hibernateOGM.HibernateOGMWorkspaceDAO;
import com.stacksync.syncservice.exceptions.dao.DAOException;
import com.stacksync.syncservice.exceptions.storage.NoStorageManagerAvailable;
import com.stacksync.syncservice.handler.Handler;
import com.stacksync.syncservice.handler.Handler.Status;
import com.stacksync.syncservice.rpc.parser.IParser;
import com.stacksync.syncservice.util.Config;
import java.io.IOException;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import javax.persistence.EntityManagerFactory;

public class HibernateOGMBasicHandler {

    private static IParser reader;
    private static WorkspaceDAO workspaceDAO;
    private static UserDAO userDao;
    private static DeviceDAO deviceDao;
    private static Handler handler;

    private static User user;
    private static Workspace workspace;
    private static Device device;
    private static ItemDAO itemDao;
    private static ItemVersionDAO itemVersionDao;
    
    private static EntityManagerFactory emf;

    @Test
    public void checkCommit() throws DAOException, SQLException, NoStorageManagerAvailable, IOException {

        Config.loadProperties("/home/marcruiz/NetBeansProjects/sync-service-new-master/config.properties");
        
        emf = HibernateOGMEntityManagerFactory.getEntityManagerFactory();
        handler = new Handler(emf);

        user = new User(null, "test", "test", "test", "test@test.com", 0L, 0L, 0L);
        UUID uuids[] = handler.createUser(user);

        user.setId(uuids[0]);
        workspace = new Workspace(uuids[1]);
        device = new Device(uuids[2]);

        ArrayList<String> chunks = new ArrayList<String>();

        chunks.add("1");
        chunks.add("2");
        chunks.add("3");

        Date date = new Timestamp(System.currentTimeMillis());

        ItemMetadata item = new ItemMetadata(null, 1L, device.getId(), null, 0L, Status.NEW.toString(), date, 0L, 0L, false, "testFile", "Mime", chunks);

        item.setTempId(UUID.randomUUID());

        ArrayList<ItemMetadata> items = new ArrayList<ItemMetadata>();

        items.add(item);

        handler.doCommit(user, workspace, device, items);
            
        workspaceDAO = new HibernateOGMWorkspaceDAO();
        
        DAOPersistenceContext persistenceContext = new DAOPersistenceContext();
        persistenceContext.setEntityManager(emf.createEntityManager());
         
        workspace = workspaceDAO.getById(workspace.getId(), persistenceContext);
        
        System.out.println("Items size: " + workspace.getItems().size());

    }
    
}
