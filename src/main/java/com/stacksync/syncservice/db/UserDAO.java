package com.stacksync.syncservice.db;

import java.util.List;
import java.util.UUID;

import com.stacksync.commons.models.User;
import com.stacksync.syncservice.exceptions.dao.DAOException;

public interface UserDAO {

	public User findById(UUID id, DAOPersistenceContext persistenceContext) throws DAOException;
	
	public User getByEmail(String email, DAOPersistenceContext persistenceContext) throws DAOException;

	public List<User> findAll(DAOPersistenceContext persistenceContext) throws DAOException;
	
	public List<User> findByItemId(UUID clientFileId, DAOPersistenceContext persistenceContext) throws DAOException;

	public void add(User user, DAOPersistenceContext persistenceContext) throws DAOException;

	public void update(User user, DAOPersistenceContext persistenceContext) throws DAOException;

	public void delete(UUID id, DAOPersistenceContext persistenceContext) throws DAOException;
	
	public void updateAvailableQuota(User user, DAOPersistenceContext persistenceContext) throws DAOException;
}
