package com.stacksync.syncservice.handler;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;

import org.apache.log4j.Logger;

import com.stacksync.commons.exceptions.ShareProposalNotCreatedException;
import com.stacksync.commons.exceptions.UserNotFoundException;
import com.stacksync.commons.models.Chunk;
import com.stacksync.commons.models.CommitInfo;
import com.stacksync.commons.models.Device;
import com.stacksync.commons.models.Item;
import com.stacksync.commons.models.ItemMetadata;
import com.stacksync.commons.models.ItemVersion;
import com.stacksync.commons.models.User;
import com.stacksync.commons.models.UserWorkspace;
import com.stacksync.commons.models.Workspace;
import com.stacksync.commons.notifications.CommitNotification;
import com.stacksync.syncservice.db.ConnectionPool;
import com.stacksync.syncservice.db.DAOFactory;
import com.stacksync.syncservice.db.DeviceDAO;
import com.stacksync.syncservice.db.ItemDAO;
import com.stacksync.syncservice.db.ItemVersionDAO;
import com.stacksync.syncservice.db.DAOPersistenceContext;
import com.stacksync.syncservice.db.UserDAO;
import com.stacksync.syncservice.db.WorkspaceDAO;
import com.stacksync.syncservice.exceptions.CommitExistantVersion;
import com.stacksync.syncservice.exceptions.CommitWrongVersion;
import com.stacksync.syncservice.exceptions.InternalServerError;
import com.stacksync.syncservice.exceptions.dao.DAOException;
import com.stacksync.syncservice.exceptions.dao.NoResultReturnedDAOException;
import com.stacksync.syncservice.exceptions.storage.NoStorageManagerAvailable;
import com.stacksync.syncservice.exceptions.storage.ObjectNotFoundException;
import com.stacksync.syncservice.storage.StorageFactory;
import com.stacksync.syncservice.storage.StorageManager;
import com.stacksync.syncservice.storage.StorageManager.StorageType;
import com.stacksync.syncservice.util.Config;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.Date;
import java.util.logging.Level;
import javax.persistence.EntityManagerFactory;
import javax.transaction.HeuristicMixedException;
import javax.transaction.HeuristicRollbackException;
import javax.transaction.NotSupportedException;
import javax.transaction.RollbackException;
import javax.transaction.SystemException;

public class Handler {

    private static final Logger logger = Logger.getLogger(Handler.class.getName());
    private transient ConnectionPool pool;
    private transient EntityManagerFactory em_pool;
    protected WorkspaceDAO workspaceDAO;
    protected UserDAO userDao;
    protected DeviceDAO deviceDao;
    protected ItemDAO itemDao;
    protected ItemVersionDAO itemVersionDao;
    protected StorageManager storageManager;

    public enum Status {

        NEW, DELETED, CHANGED, RENAMED, MOVED
    };

    public Handler(ConnectionPool pool) throws SQLException, NoStorageManagerAvailable {
        String dataSource = Config.getDatasource();

        DAOFactory factory = new DAOFactory(dataSource);

        workspaceDAO = factory.getWorkspaceDao();
        deviceDao = factory.getDeviceDAO();
        userDao = factory.getUserDao();
        itemDao = factory.getItemDAO();
        itemVersionDao = factory.getItemVersionDAO();
        this.pool = pool;
        StorageType type;
        if (Config.getSwiftKeystoneProtocol().equals("http")) {
            type = StorageType.SWIFT;
        } else {
            type = StorageType.SWIFT_SSL;
        }
        storageManager = StorageFactory.getStorageManager(type);
    }

    public Handler(EntityManagerFactory pool) throws SQLException, NoStorageManagerAvailable {
        String dataSource = Config.getDatasource();

        DAOFactory factory = new DAOFactory(dataSource);

        workspaceDAO = factory.getWorkspaceDao();
        deviceDao = factory.getDeviceDAO();
        userDao = factory.getUserDao();
        itemDao = factory.getItemDAO();
        itemVersionDao = factory.getItemVersionDAO();
        this.em_pool = pool;
        StorageType type;
        if (Config.getSwiftKeystoneProtocol().equals("http")) {
            type = StorageType.SWIFT;
        } else {
            type = StorageType.SWIFT_SSL;
        }
        storageManager = StorageFactory.getStorageManager(type);
    }

    public CommitNotification doCommit(User user, Workspace workspace, Device device, List<ItemMetadata> items)
            throws DAOException {

        DAOPersistenceContext persistenceContext = beginTransaction();

        List<CommitInfo> responseObjects = new ArrayList<CommitInfo>();

        HashMap<UUID, UUID> tempIds = new HashMap<UUID, UUID>();

        workspace = workspaceDAO.getById(workspace.getId(), persistenceContext);
        // TODO: check if the workspace belongs to the user or its been given
        // access

        device = workspace.getDevice(device.getId());
        // TODO: check if the device belongs to the user

        user = workspace.getUser(user.getId());

        for (ItemMetadata item : items) {

            ItemMetadata objectResponse = null;
            boolean committed;

            try {
                if (item.getParentId() != null) {
                    UUID parentId = tempIds.get(item.getParentId());
                    if (parentId != null) {
                        item.setParentId(parentId);
                    }
                }

                // if the item does not have ID but has a TempID, maybe it was
                // set
                if (item.getId() == null && item.getTempId() != null) {
                    UUID newId = tempIds.get(item.getTempId());
                    if (newId != null) {
                        item.setId(newId);
                    }
                }
                if (workspace.isShared()) {
                    User owner = workspace.getOwner();
                    this.commitObject(owner, item, workspace, device, persistenceContext);
                } else {
                    this.commitObject(user, item, workspace, device, persistenceContext);

                }
                
                workspaceDAO.update(workspace, persistenceContext);

                if (item.getTempId() != null) {
                    tempIds.put(item.getTempId(), item.getId());
                }

                objectResponse = item;
                committed = true;
            } catch (CommitWrongVersion e) {
                logger.info("Commit wrong version item:" + e.getItem().getId());
                Item serverObject = e.getItem();
                objectResponse = this.getCurrentServerVersion(serverObject, persistenceContext);
                commitTransaction(persistenceContext);
                committed = false;
            } catch (CommitExistantVersion e) {
                logger.info("Commit existant version item:" + e.getItem().getId());
                Item serverObject = e.getItem();
                objectResponse = this.getCurrentServerVersion(serverObject, persistenceContext);
                committed = true;
            } catch (DAOException e) {
                logger.info("Owner of shared workspace not found:" + e);
                committed = false;
            }

            responseObjects.add(new CommitInfo(item.getVersion(), committed, objectResponse));
        }

        commitTransaction(persistenceContext);

        return new CommitNotification(null, responseObjects, user.getQuotaLimit(), user.getQuotaUsedLogical());
    }

    public Workspace doShareFolder(User user, List<String> emails, Item item, boolean isEncrypted)
            throws ShareProposalNotCreatedException, UserNotFoundException, DAOException {

        DAOPersistenceContext persistenceContext = beginTransaction();
        // Check the owner
        try {
            user = userDao.findById(user.getId(), persistenceContext);
        } catch (NoResultReturnedDAOException e) {
            logger.warn(e);
            throw new UserNotFoundException(e);
        } catch (DAOException e) {
            logger.error(e);
            throw new ShareProposalNotCreatedException(e);
        }

        // Get folder metadata
        try {
            item = itemDao.findById(item.getId(), persistenceContext);
        } catch (DAOException e) {
            logger.error(e);
            throw new ShareProposalNotCreatedException(e);
        }

        if (item == null || !item.isFolder()) {
            throw new ShareProposalNotCreatedException("No folder found with the given ID.");
        }

        // Get the source workspace
        Workspace sourceWorkspace;
        try {
            sourceWorkspace = workspaceDAO.getById(item.getWorkspace().getId(), persistenceContext);
        } catch (DAOException e) {
            logger.error(e);
            throw new ShareProposalNotCreatedException(e);
        }
        if (sourceWorkspace == null) {
            throw new ShareProposalNotCreatedException("Workspace not found.");
        }

        // Check the addressees
        List<User> addressees = new ArrayList<User>();
        for (String email : emails) {
            User addressee;
            try {
                addressee = userDao.getByEmail(email, persistenceContext);
                if (!addressee.getId().equals(user.getId())) {
                    addressees.add(addressee);
                }

            } catch (IllegalArgumentException e) {
                logger.error(e);
                throw new ShareProposalNotCreatedException(e);
            } catch (DAOException e) {
                logger.warn(String.format("Email '%s' does not correspond with any user. ", email), e);
            }
        }

        if (addressees.isEmpty()) {
            throw new ShareProposalNotCreatedException("No addressees found");
        }

        Workspace workspace;

        if (sourceWorkspace.isShared()) {
            workspace = sourceWorkspace;

        } else {
            // Create the new workspace
            String container = UUID.randomUUID().toString();

            workspace = new Workspace();
            workspace.setShared(true);
            workspace.setEncrypted(isEncrypted);
            workspace.setName(item.getFilename());
            workspace.setOwner(user);
            // TO REPAIR
            //workspace.setUsers(addressees);
            workspace.setSwiftContainer(container);
            workspace.setSwiftUrl(Config.getSwiftUrl() + "/" + user.getSwiftAccount());

            // Create container in Swift
            try {
                storageManager.createNewWorkspace(workspace);
            } catch (Exception e) {
                logger.error(e);
                throw new ShareProposalNotCreatedException(e);
            }

            // Save the workspace to the DB
            try {
                workspaceDAO.add(workspace, persistenceContext);
                // add the owner to the workspace
                workspaceDAO.addUser(user, workspace, persistenceContext);

            } catch (DAOException e) {
                logger.error(e);
                throw new ShareProposalNotCreatedException(e);
            }

            // Grant user to container in Swift
            try {
                storageManager.grantUserToWorkspace(user, user, workspace);
            } catch (Exception e) {
                logger.error(e);
                throw new ShareProposalNotCreatedException(e);
            }

            // Migrate files to new workspace
            List<String> chunks;
            try {
                chunks = itemDao.migrateItem(item.getId(), workspace.getId(), persistenceContext);
            } catch (Exception e) {
                logger.error(e);
                throw new ShareProposalNotCreatedException(e);
            }

            // Move chunks to new container
            for (String chunkName : chunks) {
                try {
                    storageManager.copyChunk(sourceWorkspace, workspace, chunkName);
                    storageManager.deleteChunk(sourceWorkspace, chunkName);
                } catch (ObjectNotFoundException e) {
                    logger.error(String.format(
                            "Chunk %s not found in container %s. Could not migrate to container %s.", chunkName,
                            sourceWorkspace.getSwiftContainer(), workspace.getSwiftContainer()), e);
                } catch (Exception e) {
                    logger.error(e);
                    throw new ShareProposalNotCreatedException(e);
                }
            }
        }

        // Add the addressees to the workspace
        for (User addressee : addressees) {
            try {
                workspaceDAO.addUser(addressee, workspace, persistenceContext);

            } catch (DAOException e) {
                workspace.getUsers().remove(addressee);
                logger.error(String.format("An error ocurred when adding the user '%s' to workspace '%s'",
                        addressee.getId(), workspace.getId()), e);
            }

            // Grant the user to container in Swift
            try {
                storageManager.grantUserToWorkspace(user, addressee, workspace);
            } catch (Exception e) {
                logger.error(e);
                throw new ShareProposalNotCreatedException(e);
            }
        }

        commitTransaction(persistenceContext);

        return workspace;
    }

    public UnshareData doUnshareFolder(User user, List<String> emails, Item item, boolean isEncrypted)
            throws ShareProposalNotCreatedException, UserNotFoundException, DAOException {

        DAOPersistenceContext persistenceContext = beginTransaction();
        UnshareData response;
        // Check the owner
        try {
            user = userDao.findById(user.getId(), persistenceContext);
        } catch (NoResultReturnedDAOException e) {
            logger.warn(e);
            throw new UserNotFoundException(e);
        } catch (DAOException e) {
            logger.error(e);
            throw new ShareProposalNotCreatedException(e);
        }

        // Get folder metadata
        try {
            item = itemDao.findById(item.getId(), persistenceContext);
        } catch (DAOException e) {
            logger.error(e);
            throw new ShareProposalNotCreatedException(e);
        }

        if (item == null || !item.isFolder()) {
            throw new ShareProposalNotCreatedException("No folder found with the given ID.");
        }

        // Get the workspace
        Workspace sourceWorkspace;
        try {
            sourceWorkspace = workspaceDAO.getById(item.getWorkspace().getId(), persistenceContext);
        } catch (DAOException e) {
            logger.error(e);
            throw new ShareProposalNotCreatedException(e);
        }
        if (sourceWorkspace == null) {
            throw new ShareProposalNotCreatedException("Workspace not found.");
        }
        if (!sourceWorkspace.isShared()) {
            throw new ShareProposalNotCreatedException("This workspace is not shared.");
        }

        // Check the addressees
        List<User> addressees = new ArrayList<User>();
        for (String email : emails) {
            User addressee;
            try {
                addressee = userDao.getByEmail(email, persistenceContext);
                if (addressee.getId().equals(sourceWorkspace.getOwner().getId())) {
                    logger.warn(String.format("Email '%s' corresponds with owner of the folder. ", email));
                    throw new ShareProposalNotCreatedException("Email " + email
                            + " corresponds with owner of the folder.");

                }

                if (!addressee.getId().equals(user.getId())) {
                    addressees.add(addressee);
                }

            } catch (IllegalArgumentException e) {
                logger.error(e);
                throw new ShareProposalNotCreatedException(e);
            } catch (DAOException e) {
                logger.warn(String.format("Email '%s' does not correspond with any user. ", email), e);
            }
        }

        if (addressees.isEmpty()) {
            throw new ShareProposalNotCreatedException("No addressees found");
        }

        // get workspace members
        List<UserWorkspace> workspaceMembers;
        try {
            workspaceMembers = doGetWorkspaceMembers(user, sourceWorkspace);
        } catch (InternalServerError e1) {
            throw new ShareProposalNotCreatedException(e1.toString());
        }

        // remove users from workspace
        List<User> usersToRemove = new ArrayList<User>();

        for (User userToRemove : addressees) {
            for (UserWorkspace member : workspaceMembers) {
                if (member.getUser().getEmail().equals(userToRemove.getEmail())) {
                    workspaceMembers.remove(member);
                    usersToRemove.add(userToRemove);
                    break;
                }
            }
        }

        if (workspaceMembers.size() <= 1) {
            // All members have been removed from the workspace
            Workspace defaultWorkspace;
            try {
                // Always the last member of a shared folder should be the owner
                defaultWorkspace = workspaceDAO.getDefaultWorkspaceByUserId(sourceWorkspace.getOwner().getId(), persistenceContext);
            } catch (DAOException e) {
                logger.error(e);
                throw new ShareProposalNotCreatedException("Could not get default workspace");
            }

            // Migrate files to new workspace
            List<String> chunks;
            try {
                chunks = itemDao.migrateItem(item.getId(), defaultWorkspace.getId(), persistenceContext);
            } catch (Exception e) {
                logger.error(e);
                throw new ShareProposalNotCreatedException(e);
            }

            // Move chunks to new container
            for (String chunkName : chunks) {
                try {
                    storageManager.copyChunk(sourceWorkspace, defaultWorkspace, chunkName);
                    storageManager.deleteChunk(sourceWorkspace, chunkName);
                } catch (ObjectNotFoundException e) {
                    logger.error(String.format(
                            "Chunk %s not found in container %s. Could not migrate to container %s.", chunkName,
                            sourceWorkspace.getSwiftContainer(), defaultWorkspace.getSwiftContainer()), e);
                } catch (Exception e) {
                    logger.error(e);
                    throw new ShareProposalNotCreatedException(e);
                }
            }

            // delete workspace
            try {
                workspaceDAO.delete(sourceWorkspace.getId(), persistenceContext);
            } catch (DAOException e) {
                logger.error(e);
                throw new ShareProposalNotCreatedException(e);
            }

            // delete container from swift
            try {
                storageManager.deleteWorkspace(sourceWorkspace);
            } catch (Exception e) {
                logger.error(e);
                throw new ShareProposalNotCreatedException(e);
            }

            response = new UnshareData(usersToRemove, sourceWorkspace, true);

        } else {

            for (User userToRemove : usersToRemove) {

                try {
                    workspaceDAO.deleteUser(userToRemove, sourceWorkspace, persistenceContext);
                } catch (DAOException e) {
                    logger.error(e);
                    throw new ShareProposalNotCreatedException(e);
                }

                try {
                    storageManager.removeUserToWorkspace(user, userToRemove, sourceWorkspace);
                } catch (Exception e) {
                    logger.error(e);
                    throw new ShareProposalNotCreatedException(e);
                }
            }
            response = new UnshareData(usersToRemove, sourceWorkspace, false);

        }

        commitTransaction(persistenceContext);

        return response;
    }

    public List<UserWorkspace> doGetWorkspaceMembers(User user, Workspace workspace) throws InternalServerError, DAOException {

        DAOPersistenceContext persistenceContext = startConnection();

        // TODO: check user permissions.
        List<UserWorkspace> members;
        try {
            members = workspaceDAO.getMembersById(workspace.getId(), persistenceContext);

        } catch (DAOException e) {
            logger.error(e);
            throw new InternalServerError(e);
        }

        closeConnection(persistenceContext);

        if (members == null || members.isEmpty()) {
            throw new InternalServerError("No members found in workspace.");
        }

        return members;
    }

    /*
     * Private functions
     */
    private void commitObject(User user, ItemMetadata item, Workspace workspace, Device device, DAOPersistenceContext persistenceContext)
            throws CommitWrongVersion, CommitExistantVersion, DAOException {

        Item serverItem = workspace.getItem(item.getId());

        // Check if this object already exists in the server.
        if (serverItem == null) {
            if (item.getVersion().equals(1L)) {
                long newQuotaUsedLogical = item.getSize() + user.getQuotaUsedLogical();
                this.saveNewObject(item, workspace, device, persistenceContext);

                // Update quota used
                 user.setQuotaUsedLogical(newQuotaUsedLogical);
            } else {
                throw new CommitWrongVersion("Invalid version " + item.getVersion() + ". Expected version 1.");
            }
            return;
        }

        // Check if the client version already exists in the server
        long serverVersion = serverItem.getLatestVersion();
        long clientVersion = item.getVersion();
        boolean existVersionInServer = (serverVersion >= clientVersion);

        if (existVersionInServer) {
            this.saveExistentVersion(serverItem, item, persistenceContext);
        } else {
            // Check if version is correct
            if (serverVersion + 1 == clientVersion) {
                ItemMetadata serverItemMetadata = itemDao.findById(item.getId(), false, serverItem.getLatestVersion(),
                        false, false, persistenceContext);
                if (item.getStatus().equals(Status.DELETED.toString())) {
                    item.setSize(0L);
                }
                long newQuotaUsedLogical = user.getQuotaUsedLogical() + (item.getSize() - serverItemMetadata.getSize());

                if (newQuotaUsedLogical < 0) {
                    newQuotaUsedLogical = 0L;
                }

                this.saveNewVersion(item, serverItem, workspace, device, persistenceContext);
                logger.debug("New Quota:" + newQuotaUsedLogical);
                user.setQuotaUsedLogical(newQuotaUsedLogical);
                userDao.updateAvailableQuota(user, persistenceContext);
            } else {
                throw new CommitWrongVersion("Invalid version.", serverItem);
            }
        }
    }

    private void saveNewObject(ItemMetadata metadata, Workspace workspace, Device device, DAOPersistenceContext persistenceContext) throws DAOException {
        // Create workspace and parent instances
        UUID parentId = metadata.getParentId();
        Item parent = null;
        if (parentId != null) {
            parent = workspace.getItem(parentId);
        }

        // Insert object to DB
        Item item = new Item(UUID.randomUUID());
        item.setFilename(metadata.getFilename());
        item.setMimetype(metadata.getMimetype());
        item.setIsFolder(metadata.isFolder());
        item.setClientParentFileVersion(metadata.getParentVersion());

        item.setLatestVersion(metadata.getVersion());
        item.setWorkspace(workspace);
        if(parent!=null) item.setParent(parent);

        workspace.addItem(item);
                
        // set the global ID
        metadata.setId(item.getId());

        // Insert objectVersion
        ItemVersion objectVersion = new ItemVersion();
        objectVersion.setVersion(metadata.getVersion());
        objectVersion.setModifiedAt(metadata.getModifiedAt());
        objectVersion.setChecksum(metadata.getChecksum());
        objectVersion.setStatus(metadata.getStatus());
        objectVersion.setSize(metadata.getSize());
        
        workspace.putItemVersion(item.getId(), objectVersion);
        
        // If no folder, create new chunks and update the available quota
        if (!metadata.isFolder()) {
            long fileSize = metadata.getSize();

            List<String> chunks = metadata.getChunks();
            this.createChunks(workspace, item, chunks, objectVersion, persistenceContext);
        }

    }

    private void saveNewVersion(ItemMetadata metadata, Item serverItem, Workspace workspace, Device device, DAOPersistenceContext persistenceContext)
            throws DAOException {

        // Create new objectVersion
        ItemVersion itemVersion = new ItemVersion();
        itemVersion.setVersion(metadata.getVersion());
        itemVersion.setModifiedAt(metadata.getModifiedAt());
        itemVersion.setChecksum(metadata.getChecksum());
        itemVersion.setStatus(metadata.getStatus());
        itemVersion.setSize(metadata.getSize());

        itemVersion.setDevice(device);

        itemVersionDao.add(itemVersion, persistenceContext);

        // If no folder, create new chunks
        if (!metadata.isFolder()) {
            List<String> chunks = metadata.getChunks();
            this.createChunks(workspace, serverItem, chunks, itemVersion, persistenceContext);
        }

        // TODO To Test!!
        String status = metadata.getStatus();
        if (status.equals(Status.RENAMED.toString()) || status.equals(Status.MOVED.toString())
                || status.equals(Status.DELETED.toString())) {

            serverItem.setFilename(metadata.getFilename());

            UUID parentFileId = metadata.getParentId();
            if (parentFileId == null) {
                serverItem.setClientParentFileVersion(null);
                serverItem.setParent(null);
            } else {
                serverItem.setClientParentFileVersion(metadata.getParentVersion());
                Item parent = itemDao.findById(parentFileId, persistenceContext);
                serverItem.setParent(parent);
            }
        }

        // Update object latest version
        serverItem.setLatestVersion(metadata.getVersion());
        itemDao.put(serverItem, persistenceContext);

    }

    private void createChunks(Workspace workspace , Item item, List<String> chunksString, ItemVersion objectVersion, DAOPersistenceContext persistenceContext) throws IllegalArgumentException,
            DAOException {
        if (chunksString != null) {
            if (chunksString.size() > 0) {
                List<Chunk> chunks = new ArrayList<Chunk>();
                int i = 0;

                for (String chunkName : chunksString) {
                    Chunk chunk = new Chunk(chunkName, i);
                    chunks.add(chunk);
                    i++;
                }
                workspace.putVersionChunks(item.getId(), objectVersion.getVersion(), chunks);
            }
        }
    }

    private void saveExistentVersion(Item serverObject, ItemMetadata clientMetadata, DAOPersistenceContext persistenceContext) throws CommitWrongVersion,
            CommitExistantVersion, DAOException {

        ItemMetadata serverMetadata = this.getServerObjectVersion(serverObject, clientMetadata.getVersion(), persistenceContext);

        if (!clientMetadata.equals(serverMetadata)) {
            throw new CommitWrongVersion("Invalid version.", serverObject);
        }

        boolean lastVersion = (serverObject.getLatestVersion().equals(clientMetadata.getVersion()));

        if (!lastVersion) {
            throw new CommitExistantVersion("This version already exists.", serverObject, clientMetadata.getVersion());
        }
    }

    private ItemMetadata getCurrentServerVersion(Item serverObject, DAOPersistenceContext persistenceContext) throws DAOException {
        return getServerObjectVersion(serverObject, serverObject.getLatestVersion(), persistenceContext);
    }

    private ItemMetadata getServerObjectVersion(Item serverObject, long requestedVersion, DAOPersistenceContext persistenceContext) throws DAOException {

        ItemMetadata metadata = itemVersionDao.findByItemIdAndVersion(serverObject.getId(), requestedVersion, persistenceContext);

        return metadata;
    }

    public void doCreateUser(UUID id) {
        try {

            try {
                String[] create = new String[]{
                    "INSERT INTO user1 (id, name, swift_user, swift_account, email, quota_limit) VALUES ('" + id + "', '" + id + "', '"
                    + id + "', '" + id + "', '" + id + "@asdf.asdf', 0);",
                    "INSERT INTO workspace (id, latest_revision, owner_id, is_shared, swift_container, swift_url) VALUES ('" + id
                    + "', 0, '" + id + "', false, '" + id + "', 'STORAGEURL');",
                    "INSERT INTO workspace_user(workspace_id, user_id, workspace_name, parent_item_id) VALUES ('" + id + "', '" + id
                    + "', 'default', NULL);",
                    "INSERT INTO device (id, name, user_id, os, app_version) VALUES ('" + id + "', '" + id + "', '" + id + "', 'LINUX', 1)"};

                Statement statement;

                Connection connection = startConnection().getConnection();

                statement = connection.createStatement();

                for (String query : create) {
                    statement.executeUpdate(query);
                }

                statement.close();

                connection.close();
            } catch (SQLException e) {
                logger.error(e);
            }
        } catch (Exception e) {
            logger.error(e);
        }
    }

    protected DAOPersistenceContext beginTransaction() throws DAOException {
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

    protected void commitTransaction(DAOPersistenceContext persistenceContext) throws DAOException {
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

    protected void rollbackTransaction(DAOPersistenceContext persistenceContext) throws DAOException {
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

    protected DAOPersistenceContext startConnection() throws DAOException {
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

    protected void closeConnection(DAOPersistenceContext persistenceContext) throws DAOException {
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
    
    //TODO
    public UUID[] createUser(User user){
       Workspace workspace = null;
       Device device = null;
        try {
            DAOPersistenceContext persistenceContext = beginTransaction();
            
            workspace = new Workspace(null, 1, user, false, false);
            workspace.setSwiftContainer("1");
            workspace.setSwiftUrl("1");
            workspace.setName("name");
            
            device = new Device(null, "junitdevice", user);
            device.setId(UUID.randomUUID());
            device.setAppVersion("1.0");
            Date date = new Timestamp(System.currentTimeMillis());
            device.setLastAccessAt(date);
            device.setLastIp("192.168.1.2");
            device.setOs("Darwin");
            
            user.setId(UUID.randomUUID());
            workspace.addDevice(device);
            
            workspaceDAO.add(workspace,persistenceContext);

            commitTransaction(persistenceContext);
            
        } catch (DAOException ex) {
            logger.error(String.format("User cannot be created..."));
        }
        
        return new UUID[]{user.getId(), workspace.getId(), device.getId()};
    }
}
