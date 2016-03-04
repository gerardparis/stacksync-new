package com.stacksync.syncservice.db;

import java.util.List;
import java.util.UUID;

import com.stacksync.commons.models.Item;
import com.stacksync.commons.models.ItemMetadata;
import com.stacksync.syncservice.exceptions.dao.DAOException;

public interface ItemDAO {
	public Item findById(Long id, DAOPersistenceContext persistenceContext) throws DAOException;

	public void add(Item item, DAOPersistenceContext persistenceContext) throws DAOException;

	public void update(Item item, DAOPersistenceContext persistenceContext) throws DAOException;

	public void put(Item item, DAOPersistenceContext persistenceContext) throws DAOException;

	public void delete(Long id,DAOPersistenceContext persistenceContext) throws DAOException;

	// ItemMetadata information
	public List<ItemMetadata> getItemsByWorkspaceId(UUID workspaceId, DAOPersistenceContext persistenceContext) throws DAOException;

	public List<ItemMetadata> getItemsById(Long id, DAOPersistenceContext persistenceContext) throws DAOException;

	public ItemMetadata findById(Long id, Boolean includeList, Long version, Boolean includeDeleted, Boolean includeChunks, DAOPersistenceContext persistenceContext) throws DAOException;

	public ItemMetadata findByUserId(UUID serverUserId, Boolean includeDeleted, DAOPersistenceContext persistenceContext) throws DAOException;

	public ItemMetadata findItemVersionsById(Long id, DAOPersistenceContext persistenceContext) throws DAOException;
	
	public List<String> migrateItem(Long itemId, UUID workspaceId, DAOPersistenceContext persistenceContext) throws DAOException;

}
