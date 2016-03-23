package com.stacksync.syncservice.db.hibernateOGM;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import com.stacksync.commons.models.Chunk;
import com.stacksync.commons.models.Item;
import com.stacksync.commons.models.ItemMetadata;
import com.stacksync.commons.models.ItemVersion;
import com.stacksync.syncservice.db.DAOError;
import com.stacksync.syncservice.db.DAOPersistenceContext;
import com.stacksync.syncservice.db.DAOUtil;
import com.stacksync.syncservice.db.ItemVersionDAO;
import com.stacksync.syncservice.exceptions.dao.DAOException;
import java.sql.Timestamp;
import java.util.Date;
import java.util.UUID;
import javax.persistence.EntityManager;
import org.hibernate.HibernateException;
import org.hibernate.Session;

public class HibernateOGMItemVersionDao extends HibernateOGMDAO implements ItemVersionDAO {

    private static final Logger logger = Logger.getLogger(HibernateOGMItemVersionDao.class.getName());

    public HibernateOGMItemVersionDao() {
        super();
    }

    public static ItemMetadata fromItemAndVersionToItemMetadata(Item item, ItemVersion itemVersion,DAOPersistenceContext persistenceContext) {

        List<String> chunks = null;
        if (!item.isFolder()) {
            chunks = new ArrayList<String>();

            for (Chunk chunk : itemVersion.getChunks()) {
                chunks.add(chunk.toString());
            }
        }

        ItemMetadata itemMetadata = new ItemMetadata(item.getId(),
                itemVersion.getVersion(),
                itemVersion.getDevice().getId(),
                item.getParentId(),
                item.getClientParentFileVersion(),
                itemVersion.getStatus(),
                itemVersion.getModifiedAt(),
                itemVersion.getChecksum(),
                itemVersion.getSize(),
                item.isFolder(),
                item.getFilename(),
                item.getMimetype(),
                chunks);

        return itemMetadata;

    }

    @Override
    public ItemMetadata findByItemIdAndVersion(UUID id, Long version,DAOPersistenceContext persistenceContext) throws DAOException {
        Item item = (Item) persistenceContext.getEntityManager().find(Item.class, id);
        ItemVersion versionItem = null;

        for (ItemVersion itemVersion : item.getVersions()) {
            if (itemVersion.getVersion().equals(version)) {
                versionItem = itemVersion;
                break;
            }
        }

        return fromItemAndVersionToItemMetadata(item, versionItem, persistenceContext);
    }

    @Override
    public void add(ItemVersion itemVersion,DAOPersistenceContext persistenceContext) throws DAOException {
        if (!itemVersion.isValid()) {
            throw new IllegalArgumentException("Item version attributes not set");
        }

        Date now = new Timestamp(System.currentTimeMillis());
        itemVersion.setCommittedAt(now);
        
        persistenceContext.getEntityManager().persist(itemVersion);

    }

    @Override
    public void update(ItemVersion itemVersion, DAOPersistenceContext persistenceContext) throws DAOException {
        // TODO Auto-generated method stub

    }

    @Override
    public void delete(ItemVersion itemVersion, DAOPersistenceContext persistenceContext) throws DAOException {
        // TODO Auto-generated method stub

    }

    @Override
    public void insertChunk(ItemVersion item, UUID chunkId, Integer order, DAOPersistenceContext persistenceContext) throws DAOException {
        try {
            
            Chunk chunk = new Chunk(chunkId.toString(), order);
            persistenceContext.getEntityManager().persist(chunk);

        } catch (HibernateException e) {
            logger.error(e);
            throw new DAOException(DAOError.INTERNAL_SERVER_ERROR);
        }
    }

    @Override
    public void insertChunks(List<Chunk> chunks, ItemVersion item, DAOPersistenceContext persistenceContext) throws DAOException {
        try {
            if (chunks.isEmpty()) {
                throw new IllegalArgumentException("No chunks received");
            }

            item.setChunks(chunks);
            persistenceContext.getEntityManager().merge(item);

        } catch (HibernateException e) {
            logger.error(e);
            throw new DAOException(DAOError.INTERNAL_SERVER_ERROR);
        }

    }

    @Override
    public List<Chunk> findChunks(UUID itemVersionId, DAOPersistenceContext persistenceContext) throws DAOException {
        try {

            ItemVersion item = (ItemVersion) persistenceContext.getEntityManager().find(ItemVersion.class, itemVersionId);

            return item.getChunks();
        } catch (HibernateException e) {
            logger.error(e);
            throw new DAOException(DAOError.INTERNAL_SERVER_ERROR);
        }
    }

}
