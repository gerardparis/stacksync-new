package com.stacksync.syncservice.db.hibernateOGM;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.UUID;

import org.apache.log4j.Logger;

import com.stacksync.commons.models.Item;
import com.stacksync.commons.models.ItemMetadata;
import com.stacksync.commons.models.ItemVersion;
import com.stacksync.commons.models.Workspace;
import com.stacksync.syncservice.db.DAOError;
import com.stacksync.syncservice.db.DAOPersistenceContext;
import com.stacksync.syncservice.db.DAOUtil;
import com.stacksync.syncservice.db.ItemDAO;
import com.stacksync.syncservice.exceptions.dao.DAOException;
import javax.persistence.EntityManager;
import org.hibernate.HibernateException;

public class HibernateOGMItemDAO extends HibernateOGMDAO implements ItemDAO {

    private static final Logger logger = Logger
            .getLogger(HibernateOGMItemDAO.class.getName());

    public HibernateOGMItemDAO() {
        super();
    }

    @Override
    public Item findById(UUID itemID,DAOPersistenceContext persistenceContext) throws DAOException {
        
        if(itemID == null) return null;
        
        try {
            return (Item) persistenceContext.getEntityManager().find(Item.class, itemID);
        } catch (HibernateException e) {
            logger.error(e);
            throw new DAOException(DAOError.INTERNAL_SERVER_ERROR);
        }
    }

    @Override
    public void add(Item item,DAOPersistenceContext persistenceContext) throws DAOException {

        if (!item.isValid()) {
            throw new IllegalArgumentException("Item attributes not set");
        }
        
        item.setId(null);
        persistenceContext.getEntityManager().persist(item);

    }

    @Override
    public void put(Item item,DAOPersistenceContext persistenceContext) throws DAOException {
        
        Item itemDB = (Item) persistenceContext.getEntityManager().find(Item.class, item.getId());
        
        if (itemDB == null) {
            add(item, persistenceContext);
        } else {
            update(item, persistenceContext);
        }
    }

    @Override
    public void update(Item item,DAOPersistenceContext persistenceContext) throws DAOException {
        if (item.getId() == null || !item.isValid()) {
            throw new IllegalArgumentException("Item attributes not set");
        }

        try {

            Item itemDB = (Item) persistenceContext.getEntityManager().find(Item.class, item.getId());

            if (!item.getWorkspace().getId().equals(itemDB.getWorkspace().getId())) {
                itemDB.setWorkspace((Workspace) persistenceContext.getEntityManager().find(Workspace.class, item.getWorkspace().getId()));
            }

            itemDB.setLatestVersion(item.getLatestVersion());

            if (item.getParent() != null) {
                if (itemDB.getParent() != null) {
                    if (!item.getParent().getId().equals(itemDB.getParent().getId())) {
                        itemDB.setParent(persistenceContext.getEntityManager().find(Item.class, item.getParent().getId()));
                    }
                } else {
                    itemDB.setParent(persistenceContext.getEntityManager().find(Item.class, item.getParent().getId()));
                }
            } else {
                if (itemDB.getParent() != null) {
                    itemDB.setParent(persistenceContext.getEntityManager().find(Item.class, null));
                }
            }

            itemDB.setFilename(item.getFilename());
            itemDB.setMimetype(item.getMimetype());
            itemDB.setIsFolder(item.isFolder());
            itemDB.setClientParentFileVersion(item.getClientParentFileVersion());

            persistenceContext.getEntityManager().merge(itemDB);

        } catch (HibernateException e) {
            logger.error(e);
            throw new DAOException(DAOError.INTERNAL_SERVER_ERROR);
        }
    }

    @Override
    public void delete(UUID id,DAOPersistenceContext persistenceContext) throws DAOException {
        // TODO Auto-generated method stub

    }

    @Override
    public List<ItemMetadata> getItemsByWorkspaceId(UUID workspaceId,DAOPersistenceContext persistenceContext)
            throws DAOException {

        /* TODO, used by getChanges
        
         Object[] values = {workspaceId, workspaceId};

         String query = "WITH RECURSIVE q AS "
         + "( "
         + " SELECT i.id AS item_id, i.parent_id, i.client_parent_file_version, "
         + " i.filename, iv.id AS version_id, iv.version, i.is_folder, "
         + " i.workspace_id, "
         + " iv.size, iv.status, i.mimetype, "
         + " iv.checksum, iv.device_id, iv.modified_at, "
         + " ARRAY[i.id] AS level_array "
         + " FROM workspace w  "
         + " INNER JOIN item i ON w.id = i.workspace_id "
         + " INNER JOIN item_version iv ON i.id = iv.item_id AND i.latest_version = iv.version "
         + " WHERE w.id = ?::uuid AND i.parent_id IS NULL "
         + " UNION ALL  "
         + " SELECT i2.id AS item_id, i2.parent_id, i2.client_parent_file_version, "
         + " i2.filename, iv2.id AS version_id, iv2.version, i2.is_folder,  "
         + " i2.workspace_id, "
         + " iv2.size, iv2.status, i2.mimetype, "
         + " iv2.checksum, iv2.device_id, iv2.modified_at,  "
         + " q.level_array || i2.id "
         + " FROM q  "
         + " JOIN item i2 ON i2.parent_id = q.item_id "
         + " INNER JOIN item_version iv2 ON i2.id = iv2.item_id AND i2.latest_version = iv2.version "
         + " WHERE i2.workspace_id=?::uuid "
         + " )  "
         + " SELECT array_upper(level_array, 1) as level, q.*, get_chunks(q.version_id) AS chunks "
         + " FROM q  "
         + " ORDER BY level_array ASC";

         ResultSet result = null;
         List<ItemMetadata> items;
         try {
         result = executeQuery(query, values);

         items = new ArrayList<ItemMetadata>();

         while (result.next()) {
         ItemMetadata item = DAOUtil
         .getItemMetadataFromResultSet(result);
         items.add(item);
         }

         } catch (SQLException e) {
         logger.error(e);
         throw new DAOException(DAOError.INTERNAL_SERVER_ERROR);
         }

         return items;
         */
        return null;
    }

    @Override
    public List<ItemMetadata> getItemsById(UUID id,DAOPersistenceContext persistenceContext) throws DAOException {

        /* not used by the moment, only required for API
        
         Object[] values = {id};

         String query = "WITH    RECURSIVE "
         + " q AS  "
         + " (  "
         + " SELECT i.id AS item_id, i.parent_id, i.client_parent_file_version, "
         + " 	i.filename, iv.id AS version_id, iv.version, i.is_folder, "
         + " 	i.workspace_id, "
         + " 	iv.size, iv.status, i.mimetype, "
         + " 	iv.checksum, iv.device_id, iv.modified_at, "
         + " 	ARRAY[i.id] AS level_array "
         + " FROM    item i "
         + " INNER JOIN item_version iv ON i.id = iv.item_id AND i.latest_version = iv.version "
         + " WHERE   i.id = ? "
         + " UNION ALL "
         + " SELECT i2.id AS item_id, i2.parent_id, i2.client_parent_file_version, "
         + " 	i2.filename, iv2.id AS version_id, iv2.version, i2.is_folder,  "
         + " 	i2.workspace_id, "
         + " 	iv2.size, iv2.status, i2.mimetype, "
         + " 	iv2.checksum, iv2.device_id, iv2.modified_at,  "
         + " 	q.level_array || i2.id "
         + " FROM    q "
         + " JOIN    item i2 ON i2.parent_id = q.item_id "
         + " INNER JOIN item_version iv2 ON i2.id = iv2.item_id AND i2.latest_version = iv2.version "
         + "	) "
         + " SELECT  array_upper(level_array, 1) as level, q.* "
         + " FROM    q "
         + " ORDER BY  "
         + "       level_array ASC";

         ResultSet result = null;
         List<ItemMetadata> list = new ArrayList<ItemMetadata>();

         try {
         result = executeQuery(query, values);

         if (!resultSetHasRows(result)) {
         throw new DAOException(DAOError.FILE_NOT_FOUND);
         }

         while (result.next()) {
         ItemMetadata itemMetadata = DAOUtil
         .getItemMetadataFromResultSet(result);
         list.add(itemMetadata);
         }

         } catch (SQLException e) {
         logger.error(e);
         throw new DAOException(DAOError.INTERNAL_SERVER_ERROR);
         }

         return list;
         */
        return null;
    }

    @Override
    public ItemMetadata findById(UUID id, Boolean includeList,
            Long version, Boolean includeDeleted, Boolean includeChunks,DAOPersistenceContext persistenceContext)
            throws DAOException {

        //TO DO, recursion missing and includeDeleted
        Item item = (Item) persistenceContext.getEntityManager().find(Item.class, id);

        ItemVersion itemVersion = null;

        if (version == null) {
            version = item.getLatestVersion();
        }

        for (ItemVersion versionItem : item.getVersions()) {
            if (versionItem.getVersion().equals(version)) {
                itemVersion = versionItem;
                break;
            }
        }

        int maxLevel = includeList ? 2 : 1;
        
        return HibernateOGMItemVersionDao.fromItemAndVersionToItemMetadata(item, itemVersion, persistenceContext);
                
        /*
        
        String chunks = (includeChunks) ? ", get_chunks(%s.id) AS chunks" : "";
        // TODO: check include_deleted
        Object[] values = {id, maxLevel};

        String query = String
                .format("WITH    RECURSIVE "
                        + " q AS  "
                        + " (  "
                        + " SELECT i.id AS item_id, i.parent_id, i.client_parent_file_version, "
                        + "     i.filename, iv.version, i.is_folder, "
                        + "     iv.device_id, i.workspace_id, iv.size, iv.status, i.mimetype, "
                        + "     iv.checksum, iv.modified_at, "
                        + "     ARRAY[i.id] AS level_array "
                        + String.format(chunks, "iv")
                        + " FROM    item i "
                        + " INNER JOIN item_version iv ON i.id = iv.item_id AND %s = iv.version "
                        + " WHERE   i.id = ? "
                        + " UNION ALL "
                        + " SELECT i2.id AS item_id, i2.parent_id, i2.client_parent_file_version, "
                        + "     i2.filename, iv2.version, i2.is_folder, "
                        + "     iv2.device_id, i2.workspace_id, iv2.size, iv2.status, i2.mimetype, "
                        + "     iv2.checksum, iv2.modified_at, "
                        + "     q.level_array || i2.id "
                        + String.format(chunks, "iv2")
                        + " FROM    q "
                        + " JOIN    item i2 ON i2.parent_id = q.item_id "
                        + " INNER JOIN item_version iv2 ON i2.id = iv2.item_id AND i2.latest_version = iv2.version "
                        + " WHERE   array_upper(level_array, 1) < ? " + "	) "
                        + " SELECT  array_upper(level_array, 1) as level, q.* "
                        + " FROM    q " + " ORDER BY  "
                        + "       level_array ASC", targetVersion);

        ResultSet result = null;
        ItemMetadata item = null;

        try {
            result = executeQuery(query, values);

            if (!resultSetHasRows(result)) {
                throw new DAOException(DAOError.FILE_NOT_FOUND);
            }

            while (result.next()) {
                ItemMetadata itemMetadata = DAOUtil
                        .getItemMetadataFromResultSet(result);

                if (itemMetadata.getLevel() == 1) {
                    item = itemMetadata;
                } else {
                    // item should not be null at this point, but who knows...

                    if (item != null
                            && item.getId().equals(itemMetadata.getParentId())) {
                        if (itemMetadata.getStatus().compareTo(
                                Status.DELETED.toString()) == 0) {
                            if (includeDeleted) {
                                item.addChild(itemMetadata);
                            }
                        } else {
                            item.addChild(itemMetadata);
                        }
                    }
                }
            }

        } catch (SQLException e) {
            logger.error(e);
            throw new DAOException(DAOError.INTERNAL_SERVER_ERROR);
        }

        return item;

        */
        
    }

    @Override
    public ItemMetadata findByUserId(UUID userId,
            Boolean includeDeleted,DAOPersistenceContext persistenceContext) throws DAOException {

        /* not used by the moment, only required for API
         // TODO: check include_deleted
         Object[] values = {userId};

         String query = "WITH RECURSIVE q AS "
         + " ( "
         + "    SELECT i.id AS item_id, i.parent_id, i.client_parent_file_version, "
         + "     i.filename, iv.device_id, i.workspace_id, iv.version, i.is_folder, "
         + "     iv.size, iv.status, i.mimetype, "
         + "     iv.checksum, iv.modified_at, "
         + "     ARRAY[i.id] AS level_array, '/' AS path "
         + "     FROM user1 u  "
         + "     INNER JOIN workspace_user wu ON u.id = wu.user_id  "
         + "     INNER JOIN item i ON wu.workspace_id = i.workspace_id  "
         + "     INNER JOIN item_version iv ON i.id = iv.item_id AND i.latest_version = iv.version  "
         + "     WHERE u.id = ?::uuid AND i.parent_id IS NULL  "
         + "     UNION ALL  "
         + "     SELECT i2.id AS item_id, i2.parent_id, i2.client_parent_file_version,  "
         + "     i2.filename, iv2.device_id, i2.workspace_id, iv2.version, i2.is_folder,  "
         + "     iv2.size, iv2.status, i2.mimetype,   "
         + "     iv2.checksum, iv2.modified_at,  "
         + "     q.level_array || i2.id, q.path || q.filename::TEXT || '/'  "
         + "     FROM q  "
         + "     JOIN item i2 ON i2.parent_id = q.item_id  "
         + "     INNER JOIN item_version iv2 ON i2.id = iv2.item_id AND i2.latest_version = iv2.version  "
         + "     WHERE array_upper(level_array, 1) < 1 " + " )  "
         + " SELECT array_upper(level_array, 1) as level, q.*  "
         + " FROM q  " + " ORDER BY level_array ASC";

         ResultSet result = null;

         // create the virtual ItemMetadata for the root folder
         ItemMetadata rootMetadata = new ItemMetadata();
         rootMetadata.setIsFolder(true);
         rootMetadata.setFilename("root");
         rootMetadata.setIsRoot(true);

         try {
         result = executeQuery(query, values);

         while (result.next()) {
         ItemMetadata itemMetadata = DAOUtil
         .getItemMetadataFromResultSet(result);

         if (itemMetadata.getStatus().compareTo(
         Status.DELETED.toString()) == 0) {
         if (includeDeleted) {
         rootMetadata.addChild(itemMetadata);
         }
         } else {
         rootMetadata.addChild(itemMetadata);
         }
         }

         } catch (SQLException e) {
         logger.error(e);
         throw new DAOException(DAOError.INTERNAL_SERVER_ERROR);
         }

         return rootMetadata;
                
         */
        return null;
    }

    @Override
    public ItemMetadata findItemVersionsById(UUID fileId,DAOPersistenceContext persistenceContext) throws DAOException {


        /* not used by the moment, only required for API
        
         // TODO: check include_deleted
         Object[] values = {fileId};

         String query = "SELECT i.id AS item_id, i.parent_id, i.client_parent_file_version, i.filename, i.is_folder, i.mimetype, i.workspace_id, "
         + " iv.version, iv.size, iv.status, iv.checksum, iv.device_id, "
         + " iv.modified_at, '1' AS level, '' AS path FROM item i "
         + " inner join item_version iv on iv.item_id = i.id  where i.id = ? ORDER BY iv.version DESC ";

         ResultSet result = null;

         // create the virtual ItemMetadata for the root folder
         ItemMetadata rootMetadata = new ItemMetadata();

         try {
         result = executeQuery(query, values);

         while (result.next()) {
         ItemMetadata itemMetadata = DAOUtil
         .getItemMetadataFromResultSet(result);

         if (rootMetadata.getChildren().isEmpty()) {
         rootMetadata = itemMetadata;
         }
         rootMetadata.addChild(itemMetadata);
         }

         } catch (SQLException e) {
         logger.error(e);
         throw new DAOException(DAOError.INTERNAL_SERVER_ERROR);
         }

         return rootMetadata;
                
         */
        return null;
    }

    private boolean resultSetHasRows(ResultSet resultSet,DAOPersistenceContext persistenceContext) {
        boolean hasRows = false;
        if (resultSet != null) {
            try {
                // true if the cursor is before the first row; false if the
                // cursor is at any other position or the result set contains no
                // rows }
                hasRows = resultSet.isBeforeFirst();
            } catch (SQLException e) {
            }
        }
        return hasRows;
    }

    @Override
    public List<String> migrateItem(UUID itemId, UUID workspaceId,DAOPersistenceContext persistenceContext) throws DAOException {

        /* TODO, used when sharing
        
         Object[] values = {itemId, workspaceId.toString()};

         // This query move items to the new workspace.
         String query = "WITH    RECURSIVE "
         + " q AS "
         + " ( "
         + " SELECT i.* "
         + " FROM    item i "
         + " WHERE   i.id = ? "
         + " UNION ALL "
         + " SELECT i2.* "
         + " FROM    q "
         + " JOIN    item i2 ON i2.parent_id = q.id "
         + " ) "
         + " UPDATE item i3 SET workspace_id = ?::uuid "
         + " FROM q "
         + " WHERE q.id = i3.id";

         executeUpdate(query, values);

         List<String> chunksToMigrate;

         try {
         chunksToMigrate = getChunksToMigrate(itemId);
         } catch (SQLException e) {
         throw new DAOException(e);
         }

         return chunksToMigrate;
                
         */
        return null;

    }

    private List<String> getChunksToMigrate(Long itemId,DAOPersistenceContext persistenceContext) throws DAOException, SQLException {

        /* not used function
        
         Object[] values = {itemId};

         String query = "SELECT get_unique_chunks_to_migrate(?) AS chunks";

         ResultSet result = executeQuery(query, values);
         List<String> chunksList;

         if (result.next()) {
         chunksList = DAOUtil.getArrayFromResultSet(result, "chunks");
         } else {
         chunksList = new ArrayList<String>();
         }

         return chunksList;
         */
        return null;
    }

}
