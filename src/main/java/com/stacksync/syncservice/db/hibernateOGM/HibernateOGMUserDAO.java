package com.stacksync.syncservice.db.hibernateOGM;

import com.stacksync.commons.models.Item;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.apache.log4j.Logger;

import com.stacksync.commons.models.User;
import com.stacksync.syncservice.db.DAOError;
import com.stacksync.syncservice.db.DAOPersistenceContext;
import com.stacksync.syncservice.db.UserDAO;
import com.stacksync.syncservice.exceptions.dao.DAOException;
import com.stacksync.syncservice.exceptions.dao.NoResultReturnedDAOException;
import javax.persistence.EntityManager;
import javax.persistence.Query;
import org.hibernate.HibernateException;

public class HibernateOGMUserDAO extends HibernateOGMDAO implements UserDAO {

    private static final Logger logger = Logger.getLogger(HibernateOGMUserDAO.class.getName());

    public HibernateOGMUserDAO() {
        super();
    }

    @Override
    public User findById(UUID userID,DAOPersistenceContext persistenceContext) throws DAOException {

        User user;
        try {

            user = (User) persistenceContext.getEntityManager().find(User.class, userID);

        } catch (HibernateException e) {
            logger.error(e);
            throw new DAOException(DAOError.INTERNAL_SERVER_ERROR);
        }

        if (user == null) {
            throw new DAOException(DAOError.USER_NOT_FOUND);
        }

        return user;
    }

    @Override
    public User getByEmail(String email,DAOPersistenceContext persistenceContext) throws DAOException {

        try {

            Query query = persistenceContext.getEntityManager().createQuery("from User where email = :email ");
            query.setParameter("email", email);
            List<User> list = query.getResultList();

            if (list.isEmpty()) {
                throw new NoResultReturnedDAOException(DAOError.USER_NOT_FOUND);
            }

            return (User) list.get(0);

        } catch (HibernateException e) {
            logger.error(e);
            throw new DAOException(DAOError.INTERNAL_SERVER_ERROR);
        }

    }

    @Override
    public List<User> findAll(DAOPersistenceContext persistenceContext) throws DAOException {

        try {

            Query query = persistenceContext.getEntityManager().createQuery("from User");

            List<User> list = new ArrayList<User>();
            for (final Object o : query.getResultList()) {
                list.add((User) o);
            }

            return list;

        } catch (HibernateException e) {
            logger.error(e);
            throw new DAOException(DAOError.INTERNAL_SERVER_ERROR);
        }

    }

    @Override
    public void add(User user,DAOPersistenceContext persistenceContext) throws DAOException {
        if (!user.isValid()) {
            throw new IllegalArgumentException("User attributes not set");
        }

        try {
            persistenceContext.getEntityManager().persist(user);
        } catch (HibernateException e) {
            logger.error(e);
            throw new DAOException(DAOError.INTERNAL_SERVER_ERROR);
        }
    }

    @Override
    public void update(User user,DAOPersistenceContext persistenceContext) throws DAOException {
        if (user.getId() == null || !user.isValid()) {
            throw new IllegalArgumentException("User attributes not set");
        }

        try {
            User userDB = (User) persistenceContext.getEntityManager().find(User.class, user.getId());

            userDB.setEmail(user.getEmail());
            userDB.setName(user.getName());
            userDB.setSwiftUser(user.getSwiftUser());
            userDB.setSwiftAccount(user.getSwiftAccount());
            userDB.setQuotaLimit(user.getQuotaLimit());
            userDB.setQuotaUsedLogical(user.getQuotaUsedLogical());
            userDB.setQuotaUsedReal(user.getQuotaUsedReal());

            persistenceContext.getEntityManager().merge(userDB);
        } catch (HibernateException e) {
            logger.error(e);
            throw new DAOException(DAOError.INTERNAL_SERVER_ERROR);
        }
    }

    @Override
    public void delete(UUID userID,DAOPersistenceContext persistenceContext) throws DAOException {

        User userDB = (User) persistenceContext.getEntityManager().find(User.class, userID);

        persistenceContext.getEntityManager().remove(userDB);

    }

    @Override
    public List<User> findByItemId(UUID itemId,DAOPersistenceContext persistenceContext) throws DAOException {

        try {
            
            Item item = (Item) persistenceContext.getEntityManager().find(Item.class, itemId);

            return item.getWorkspace().getUsers();
            
        } catch (HibernateException e) {
            logger.error(e);
            throw new DAOException(DAOError.INTERNAL_SERVER_ERROR);
        }
    }

    @Override
    public void updateAvailableQuota(User user,DAOPersistenceContext persistenceContext) throws DAOException {
        update(user, persistenceContext);
    }

}
