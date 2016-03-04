package com.stacksync.syncservice.db;

import java.util.UUID;

import com.stacksync.commons.models.Device;
import com.stacksync.syncservice.exceptions.dao.DAOException;

public interface DeviceDAO {
	
	public Device get(UUID id, DAOPersistenceContext persistenceContext) throws DAOException;

	public void add(Device device, DAOPersistenceContext persistenceContext) throws DAOException;

	public void update(Device device, DAOPersistenceContext persistenceContext) throws DAOException;

	public void delete(UUID id, DAOPersistenceContext persistenceContext) throws DAOException;

}
