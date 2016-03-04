package com.stacksync.syncservice.db;

import java.util.List;
import java.util.UUID;

import com.stacksync.commons.models.User;
import com.stacksync.commons.models.UserWorkspace;
import com.stacksync.commons.models.Workspace;
import com.stacksync.syncservice.exceptions.dao.DAOException;

public interface WorkspaceDAO {

	public Workspace getById(UUID id, DAOPersistenceContext persistenceContext) throws DAOException;

	public List<Workspace> getByUserId(UUID userId, DAOPersistenceContext persistenceContext) throws DAOException;
	
	public Workspace getDefaultWorkspaceByUserId(UUID userId, DAOPersistenceContext persistenceContext) throws DAOException;
	
	public Workspace getByItemId(Long itemId, DAOPersistenceContext persistenceContext) throws DAOException;

	public void add(Workspace workspace, DAOPersistenceContext persistenceContext) throws DAOException;

	public void update(User user, Workspace workspace, DAOPersistenceContext persistenceContext) throws DAOException;

	public void addUser(User user, Workspace workspace, DAOPersistenceContext persistenceContext) throws DAOException;
	
	public void deleteUser(User user, Workspace workspace, DAOPersistenceContext persistenceContext) throws DAOException;

	public void delete(UUID id, DAOPersistenceContext persistenceContext) throws DAOException;
	
	public List<UserWorkspace> getMembersById(UUID workspaceId, DAOPersistenceContext persistenceContext) throws DAOException;

}
