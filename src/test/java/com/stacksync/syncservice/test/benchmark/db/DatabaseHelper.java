package com.stacksync.syncservice.test.benchmark.db;

import java.util.List;
import java.util.UUID;

import com.stacksync.commons.models.Device;
import com.stacksync.commons.models.Item;
import com.stacksync.commons.models.ItemVersion;
import com.stacksync.commons.models.User;
import com.stacksync.commons.models.Workspace;
import com.stacksync.syncservice.db.ConnectionPool;
import com.stacksync.syncservice.db.ConnectionPoolFactory;
import com.stacksync.syncservice.db.DAOFactory;
import com.stacksync.syncservice.db.DAOPersistenceContext;
import com.stacksync.syncservice.db.DeviceDAO;
import com.stacksync.syncservice.db.ItemDAO;
import com.stacksync.syncservice.db.ItemVersionDAO;
import com.stacksync.syncservice.db.UserDAO;
import com.stacksync.syncservice.db.WorkspaceDAO;
import com.stacksync.syncservice.db.hibernateOGM.HibernateOGMEntityManagerFactory;
import com.stacksync.syncservice.exceptions.dao.DAOException;
import com.stacksync.syncservice.util.Config;
import java.sql.SQLException;
import javax.persistence.EntityManagerFactory;
import javax.transaction.HeuristicMixedException;
import javax.transaction.HeuristicRollbackException;
import javax.transaction.NotSupportedException;
import javax.transaction.RollbackException;
import javax.transaction.SystemException;

public class DatabaseHelper {
	private ConnectionPool pool;
        private transient EntityManagerFactory em_pool;
	private WorkspaceDAO workspaceDAO;
	private UserDAO userDao;
	private DeviceDAO deviceDao;
	private ItemDAO objectDao;
	private ItemVersionDAO oversionDao;

        public DatabaseHelper(ConnectionPool pool) throws Exception {
		Config.loadProperties();
		Thread.sleep(100);

		String datasource = Config.getDatasource();

		this.pool = pool;

		DAOFactory factory = new DAOFactory(datasource);

		workspaceDAO = factory.getWorkspaceDao();
		userDao = factory.getUserDao();
		deviceDao = factory.getDeviceDAO();
		objectDao = factory.getItemDAO();
		oversionDao = factory.getItemVersionDAO();
	}
                
	public DatabaseHelper() throws Exception {
		Config.loadProperties();
		Thread.sleep(100);

		String datasource = Config.getDatasource();

                if(datasource.equals("hibernateOGM")){
                    em_pool = HibernateOGMEntityManagerFactory.getEntityManagerFactory();
                } else {
                   pool = ConnectionPoolFactory.getConnectionPool(datasource); 
                }

		DAOFactory factory = new DAOFactory(datasource);

		workspaceDAO = factory.getWorkspaceDao();
		userDao = factory.getUserDao();
		deviceDao = factory.getDeviceDAO();
		objectDao = factory.getItemDAO();
		oversionDao = factory.getItemVersionDAO();
	}

	public void storeObjects(List<Item> objectsLevel) throws IllegalArgumentException, DAOException {

            /*
		long numChunk = 0, totalTimeChunk = 0;
		long numVersion = 0, totalTimeVersion = 0;
		long numObject = 0, totalTimeObject = 0;

                DAOPersistenceContext persistenceContext = beginTransaction();
                
		long startTotal = System.currentTimeMillis();
		for (Item object : objectsLevel) {
			// System.out.println("DatabaseHelper -- Put Object -> " + object);
			long startObjectTotal = System.currentTimeMillis();

			objectDao.put(object, persistenceContext);
                        //TODO
			for (ItemVersion version : object.getVersions()) {

				long startVersionTotal = System.currentTimeMillis();
				// System.out.println("DatabaseHelper -- Put Version -> " +
				// version);
				oversionDao.add(version, persistenceContext);

				long startChunkTotal = System.currentTimeMillis();

				if (!version.getChunks().isEmpty()) {
					oversionDao.insertChunks(version.getChunks(), version, persistenceContext);
				}

				totalTimeChunk += System.currentTimeMillis() - startChunkTotal;

				totalTimeVersion += System.currentTimeMillis() - startVersionTotal;
				// System.out.println("---- Total Version time --> " +
				// totalVersionTime + " ms");
				numVersion++;
			}
                        

			totalTimeObject += System.currentTimeMillis() - startObjectTotal;
			numObject++;
		}

		if (numChunk > 0) {
			System.out.println("-------- AVG avg Chunk(" + numChunk + ") time --> " + (totalTimeChunk / numChunk) + " ms");
		}

		if (numVersion > 0) {
			System.out.println("---- AVG Version(" + numVersion + ") time --> " + (totalTimeVersion / numVersion) + " ms");
		}

		if (numObject > 0) {
			System.out.println("AVG Object(" + numObject + ") time --> " + (totalTimeObject / numObject) + " ms");
		}

                commitTransaction(persistenceContext);
                
		long totalTime = System.currentTimeMillis() - startTotal;

		System.out.println("Total level time --> " + totalTime + " ms");
            
                */

	}

	public void addUser(User user, DAOPersistenceContext persistenceContext) throws IllegalArgumentException, DAOException {
		userDao.add(user,persistenceContext);
	}

	public void addWorkspace(User user, Workspace workspace, DAOPersistenceContext persistenceContext) throws IllegalArgumentException, DAOException {
		workspaceDAO.add(workspace, persistenceContext);
		workspaceDAO.addUser(user, workspace,persistenceContext);
	}

	public void addDevice(Device device, DAOPersistenceContext persistenceContext) throws IllegalArgumentException, DAOException {
		deviceDao.add(device, persistenceContext);
	}

	public void deleteUser(UUID id, DAOPersistenceContext persistenceContext) throws DAOException {
		userDao.delete(id, persistenceContext);
	}
        
    public DAOPersistenceContext beginTransaction() throws DAOException {
        try {

            DAOPersistenceContext persistenceContext = new DAOPersistenceContext();

            if (em_pool != null) {
                persistenceContext.beginTransaction(em_pool);
            } else {
                persistenceContext.beginTransaction(pool);
            }

            return persistenceContext;

        } catch (SQLException e) {
            throw new DAOException(e);
        } catch (NotSupportedException ex) {
            throw new DAOException(ex);
        } catch (SystemException ex) {
            throw new DAOException(ex);
        }
    }

    public void commitTransaction(DAOPersistenceContext persistenceContext) throws DAOException {
        try {
            persistenceContext.commitTransaction();
        } catch (SQLException e) {
            rollbackTransaction(persistenceContext);
            throw new DAOException(e);
        } catch (RollbackException ex) {
            rollbackTransaction(persistenceContext);
        } catch (HeuristicMixedException ex) {
            rollbackTransaction(persistenceContext);
        } catch (HeuristicRollbackException ex) {
            rollbackTransaction(persistenceContext);
        } catch (SecurityException ex) {
            rollbackTransaction(persistenceContext);
        } catch (IllegalStateException ex) {
            rollbackTransaction(persistenceContext);
        } catch (SystemException ex) {
            rollbackTransaction(persistenceContext);
        }
    }

    public void rollbackTransaction(DAOPersistenceContext persistenceContext) throws DAOException {
        try {
            persistenceContext.rollBackTransaction();
        } catch (SQLException e) {
            throw new DAOException(e);
        } catch (IllegalStateException ex) {
            throw new DAOException(ex);
        } catch (SecurityException ex) {
            throw new DAOException(ex);
        } catch (SystemException ex) {
            throw new DAOException(ex);
        }
    }

    public DAOPersistenceContext startConnection() throws DAOException {
        try {
            DAOPersistenceContext persistenceContext = new DAOPersistenceContext();
            if (em_pool != null) {
                persistenceContext.setEntityManager(em_pool.createEntityManager());
            } else {
                persistenceContext.setConnection(pool.getConnection());
            }

            return persistenceContext;
        } catch (SQLException e) {
            throw new DAOException(e);
        }
    }

    public void closeConnection(DAOPersistenceContext persistenceContext) throws DAOException {
        try {
            if (em_pool != null) {
                persistenceContext.closeEntityManager();
            } else{
                persistenceContext.closeConnection(); 
            }
        } catch (SQLException e) {
            throw new DAOException(e);
        }
    }
}
