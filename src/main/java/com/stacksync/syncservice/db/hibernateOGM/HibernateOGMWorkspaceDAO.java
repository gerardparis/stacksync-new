package com.stacksync.syncservice.db.hibernateOGM;

import java.util.List;
import java.util.UUID;

import org.apache.log4j.Logger;

import com.stacksync.commons.models.Item;
import com.stacksync.commons.models.User;
import com.stacksync.commons.models.UserWorkspace;
import com.stacksync.commons.models.Workspace;
import com.stacksync.syncservice.db.DAOError;
import com.stacksync.syncservice.db.DAOPersistenceContext;
import com.stacksync.syncservice.db.WorkspaceDAO;
import com.stacksync.syncservice.exceptions.dao.DAOException;
import org.hibernate.HibernateException;

public class HibernateOGMWorkspaceDAO extends HibernateOGMDAO implements WorkspaceDAO {

    private static final Logger logger = Logger.getLogger(HibernateOGMWorkspaceDAO.class.getName());

    public HibernateOGMWorkspaceDAO() {
    }

    @Override
    public Workspace getById(UUID workspaceID, DAOPersistenceContext persistenceContext) throws DAOException {
        try {
            return (Workspace) persistenceContext.getEntityManager().find(Workspace.class, workspaceID);
        } catch (HibernateException e) {
            logger.error(e);
            throw new DAOException(DAOError.INTERNAL_SERVER_ERROR);
        }

    }

    @Override
    public List<Workspace> getByUserId(UUID userId, DAOPersistenceContext persistenceContext) throws DAOException {
        /*try {

            User user = (User) persistenceContext.getEntityManager().find(User.class, userId);

            List<Workspace> workspaces = user.getWorkspaces();

            if (workspaces.isEmpty()) {
                throw new NoResultReturnedDAOException(DAOError.WORKSPACES_NOT_FOUND);
            }

            UserWorkspace userWorkspace;

            for (Workspace workspace : workspaces) {
                userWorkspace = new UserWorkspace(user, workspace);

                userWorkspace = (UserWorkspace) persistenceContext.getEntityManager().find(UserWorkspace.class, userWorkspace);
                workspace.setName(userWorkspace.getName());
                workspace.setParentItem(userWorkspace.getParentItem());
            }

            return workspaces;

        } catch (HibernateException e) {
            logger.error(e);
            throw new DAOException(DAOError.INTERNAL_SERVER_ERROR);
        }*/
        
        return null;
    }

    @Override
    public Workspace getDefaultWorkspaceByUserId(UUID userId, DAOPersistenceContext persistenceContext) throws DAOException {

        /*
            try {
            User user = (User) persistenceContext.getEntityManager().find(User.class, userId);

            List<Workspace> workspaces = user.getWorkspaces();

            Workspace result = null;

            //Found the first workspace is not shared
            for (Workspace workspace : workspaces) {

                if (!workspace.isShared()) {
                    UserWorkspace userWorkspace = new UserWorkspace(user, workspace);
                    userWorkspace = (UserWorkspace) persistenceContext.getEntityManager().find(UserWorkspace.class, userWorkspace);
                    result = workspace;
                    result.setName(userWorkspace.getName());
                    result.setParentItem(userWorkspace.getParentItem());
                    break;
                }
            }

            return result;
        } catch (HibernateException e) {
            logger.error(e);
            throw new DAOException(DAOError.INTERNAL_SERVER_ERROR);
        }
        */
        return null;
    }

    @Override
    public void add(Workspace workspace, DAOPersistenceContext persistenceContext) throws DAOException {
        try {
            if (!workspace.isValid()) {
                throw new IllegalArgumentException("Workspace attributes not set");
            }

            persistenceContext.getEntityManager().persist(workspace);

        } catch (HibernateException e) {
            logger.error(e);
            throw new DAOException(DAOError.INTERNAL_SERVER_ERROR);
        }
    }
    
    @Override
    public void update(Workspace workspace,DAOPersistenceContext persistenceContext) throws DAOException {
        persistenceContext.getEntityManager().merge(workspace);
    }

    @Override
    public void update(User user, Workspace workspace,DAOPersistenceContext persistenceContext) throws DAOException {
        /*if (workspace.getId() == null || user.getId() == null) {
            throw new IllegalArgumentException("Attributes not set");
        }

        Calendar calendar = Calendar.getInstance();
        Date now = calendar.getTime();

        UserWorkspace userWorkspace = new UserWorkspace(user, workspace);
        userWorkspace = (UserWorkspace) persistenceContext.getEntityManager().find(UserWorkspace.class, userWorkspace);
        userWorkspace.setModifiedAt(now);

        if (workspace.getParentItem() != null) {
            Item parentItem = (Item) persistenceContext.getEntityManager().find(Item.class, workspace.getParentItem().getId());
            userWorkspace.setParentItem(parentItem);
        }

        userWorkspace.setName(workspace.getName());

        */
    }

    @Override
    public void delete(UUID workspaceID,DAOPersistenceContext persistenceContext) throws DAOException {
        Workspace workspace = (Workspace) persistenceContext.getEntityManager().find(Workspace.class, workspaceID);
        persistenceContext.getEntityManager().remove(workspace);
    }

    @Override
    public void addUser(User user, Workspace workspace,DAOPersistenceContext persistenceContext) throws DAOException {
        /*
        if (user == null || !user.isValid()) {
            throw new IllegalArgumentException("User not valid");
        } else if (workspace == null || !workspace.isValid()) {
            throw new IllegalArgumentException("Workspace not valid");
        }

        Item parentItem = null;
        if (workspace.getParentItem() != null) {
            parentItem = (Item) persistenceContext.getEntityManager().find(Item.class, workspace.getParentItem().getId());
        }

        user = (User) persistenceContext.getEntityManager().find(User.class, user.getId()); 
        
        workspace = (Workspace) persistenceContext.getEntityManager().find(Workspace.class, workspace.getId());
        
        workspace.setOwner(user);
        user.setUserWorkspaces(workspace);
        
        persistenceContext.getEntityManager().merge(workspace);
        */
        
    }

    @Override
    public void deleteUser(User user, Workspace workspace, DAOPersistenceContext persistenceContext) throws DAOException {

        /*
        if (user == null || !user.isValid()) {
            throw new IllegalArgumentException("User not valid");
        } else if (workspace == null || !workspace.isValid()) {
            throw new IllegalArgumentException("Workspace not valid");
        }

        UserWorkspace userWorkspace = new UserWorkspace(user, workspace);
        userWorkspace = (UserWorkspace) persistenceContext.getEntityManager().find(UserWorkspace.class, userWorkspace);

        if(userWorkspace!=null)
            persistenceContext.getEntityManager().remove(userWorkspace);
        */
    }

    @Override
    public Workspace getByItemId(UUID itemId, DAOPersistenceContext persistenceContext) throws DAOException {

        /*Item item = (Item) persistenceContext.getEntityManager().find(Item.class, itemId);

        return item.getWorkspace();*/
        return null;
    }

    @Override
    public List<UserWorkspace> getMembersById(UUID workspaceId, DAOPersistenceContext persistenceContext) throws DAOException {

        //Workspace workspace = (Workspace) persistenceContext.getEntityManager().find(Workspace.class, workspaceId);

        //return workspace.getWorkspaceUsers();

        return null;
    }

}
