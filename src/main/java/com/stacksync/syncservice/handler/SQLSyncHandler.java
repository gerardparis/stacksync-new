package com.stacksync.syncservice.handler;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.apache.log4j.Logger;

import com.stacksync.commons.models.Device;
import com.stacksync.commons.models.ItemMetadata;
import com.stacksync.commons.models.User;
import com.stacksync.commons.models.Workspace;
import com.stacksync.syncservice.db.ConnectionPool;
import com.stacksync.commons.exceptions.DeviceNotUpdatedException;
import com.stacksync.commons.exceptions.DeviceNotValidException;
import com.stacksync.commons.exceptions.NoWorkspacesFoundException;
import com.stacksync.commons.exceptions.UserNotFoundException;
import com.stacksync.commons.exceptions.WorkspaceNotUpdatedException;
import com.stacksync.syncservice.db.DAOPersistenceContext;
import com.stacksync.syncservice.exceptions.dao.DAOException;
import com.stacksync.syncservice.exceptions.dao.NoResultReturnedDAOException;
import com.stacksync.syncservice.exceptions.dao.NoRowsAffectedDAOException;
import com.stacksync.syncservice.exceptions.storage.NoStorageManagerAvailable;
import java.util.logging.Level;
import javax.persistence.EntityManagerFactory;

public class SQLSyncHandler extends Handler implements SyncHandler {

	private static final Logger logger = Logger.getLogger(SQLSyncHandler.class.getName());	

	public SQLSyncHandler(ConnectionPool pool) throws SQLException, NoStorageManagerAvailable {
		super(pool);
	}
        
        public SQLSyncHandler(EntityManagerFactory pool) throws SQLException, NoStorageManagerAvailable {
		super(pool);
	}

	@Override
	public List<ItemMetadata> doGetChanges(User user, Workspace workspace) {
		List<ItemMetadata> responseObjects = new ArrayList<ItemMetadata>();

		try {
                        DAOPersistenceContext persistenceContext = startConnection();
			
                        responseObjects = itemDao.getItemsByWorkspaceId(workspace.getId(), persistenceContext);

                        closeConnection(persistenceContext);
                                
		} catch (DAOException e) {
			logger.error(e.toString(), e);
		}
                
                
		return responseObjects;
	}

	@Override
	public List<Workspace> doGetWorkspaces(User user) throws NoWorkspacesFoundException {

		List<Workspace> workspaces = new ArrayList<Workspace>();

		try {
                        DAOPersistenceContext persistenceContext = startConnection();
                        
			workspaces = workspaceDAO.getByUserId(user.getId(),persistenceContext);
                        
                        closeConnection(persistenceContext);
                        
		} catch (NoResultReturnedDAOException e) {
			logger.error(e);
			throw new NoWorkspacesFoundException(String.format("No workspaces found for user: %s", user.getId()));
		} catch (DAOException e) {
			logger.error(e);
			throw new NoWorkspacesFoundException(e);
		}

		return workspaces;
	}

	@Override
	public UUID doUpdateDevice(Device device) throws UserNotFoundException, DeviceNotValidException,
			DeviceNotUpdatedException {

                
                DAOPersistenceContext persistenceContext;
		try {	
                        persistenceContext = beginTransaction();
                        User dbUser = userDao.findById(device.getUser().getId(),persistenceContext);
			device.setUser(dbUser);

		} catch (NoResultReturnedDAOException e) {
			logger.warn(e);
			throw new UserNotFoundException(e);
		} catch (DAOException e) {
			logger.error(e);
			throw new DeviceNotUpdatedException(e);
		}

		try {
			if (device.getId() == null) {
				deviceDao.add(device,persistenceContext);
			} else {
				deviceDao.update(device,persistenceContext);
			}
                
                        commitTransaction(persistenceContext);
                        
		} catch (NoRowsAffectedDAOException e) {
			logger.error(e);
			throw new DeviceNotUpdatedException(e);
		} catch (DAOException e) {
			logger.error(e);
			throw new DeviceNotUpdatedException(e);
		} catch (IllegalArgumentException e) {
			logger.error(e);
			throw new DeviceNotValidException(e);
		}

		return device.getId();
	}
	

	@Override
	public void doUpdateWorkspace(User user, Workspace workspace) throws UserNotFoundException,
			WorkspaceNotUpdatedException {

                DAOPersistenceContext persistenceContext;
		// Check the owner
		try {
                        persistenceContext = beginTransaction();
			user = userDao.findById(user.getId(),persistenceContext);
		} catch (NoResultReturnedDAOException e) {
			logger.warn(e);
			throw new UserNotFoundException(e);
		} catch (DAOException e) {
			logger.error(e);
			throw new WorkspaceNotUpdatedException(e);
		}

		// Update the workspace
		try {
			workspaceDAO.update(user, workspace,persistenceContext);
                        commitTransaction(persistenceContext);
		} catch (NoRowsAffectedDAOException e) {
			logger.error(e);
			throw new WorkspaceNotUpdatedException(e);
		} catch (DAOException e) {
			logger.error(e);
			throw new WorkspaceNotUpdatedException(e);
		}
	}

	@Override
	public User doGetUser(String email) throws UserNotFoundException {

		try {
                        DAOPersistenceContext persistenceContext = startConnection();
			User user = userDao.getByEmail(email,persistenceContext);
                        closeConnection(persistenceContext);
			return user;

		} catch (NoResultReturnedDAOException e) {
			logger.error(e);
			throw new UserNotFoundException(e);
		} catch (DAOException e) {
			logger.error(e);
			throw new UserNotFoundException(e);
		}
	}
      
}
