package com.stacksync.syncservice.db;

import java.util.List;

import com.stacksync.commons.models.Chunk;
import com.stacksync.commons.models.ItemMetadata;
import com.stacksync.commons.models.ItemVersion;
import com.stacksync.syncservice.exceptions.dao.DAOException;
import java.util.UUID;

public interface ItemVersionDAO {

	public ItemMetadata findByItemIdAndVersion(UUID id, Long version, DAOPersistenceContext persistenceContext) throws DAOException;;

	public void add(ItemVersion itemVersion, DAOPersistenceContext persistenceContext) throws DAOException;

	public void insertChunk(ItemVersion item, UUID chunkId, Integer order, DAOPersistenceContext persistenceContext) throws DAOException;

	public void insertChunks(List<Chunk> chunks, ItemVersion item, DAOPersistenceContext persistenceContext) throws DAOException;

	public List<Chunk> findChunks(UUID itemVersionId, DAOPersistenceContext persistenceContext) throws DAOException;

	public void update(ItemVersion itemVersion, DAOPersistenceContext persistenceContext) throws DAOException;

	public void delete(ItemVersion itemVersion, DAOPersistenceContext persistenceContext) throws DAOException;
}
