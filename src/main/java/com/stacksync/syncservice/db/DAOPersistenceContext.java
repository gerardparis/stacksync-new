/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.stacksync.syncservice.db;

import java.sql.Connection;
import java.sql.SQLException;
import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.transaction.HeuristicMixedException;
import javax.transaction.HeuristicRollbackException;
import javax.transaction.NotSupportedException;
import javax.transaction.RollbackException;
import javax.transaction.SystemException;
import javax.transaction.TransactionManager;

/**
 *
 * @author marcruiz
 */
public class DAOPersistenceContext {

    Connection connection;
    EntityManager em;
    TransactionManager tm;

    public void beginTransaction(ConnectionPool pool) throws SQLException {
        connection = pool.getConnection();
        connection.setAutoCommit(false);
    }

    public void beginTransaction(EntityManagerFactory pool) throws SQLException, NotSupportedException, SystemException {
        tm = com.arjuna.ats.jta.TransactionManager.transactionManager();
        tm.begin();
        em = pool.createEntityManager();
    }

    public void commitTransaction() throws SQLException, RollbackException, HeuristicMixedException, HeuristicRollbackException, SecurityException, IllegalStateException, SystemException {
        if (connection != null) {
            connection.commit();
            connection.setAutoCommit(true);
            connection.close();
        } else if (tm != null) {
            em.flush();
            em.close();
            tm.commit();
        }
    }

    public void rollBackTransaction() throws SQLException, IllegalStateException, SecurityException, SystemException {
        if (connection != null) {
            connection.rollback();
            connection.setAutoCommit(true);
            connection.close();
        } else if (tm != null) {
            tm.rollback();
        }
    }

    public Connection getConnection() {
        return connection;
    }
    
    public void setConnection(Connection connection) {
        this.connection = connection;
    }
    
    public void closeConnection() throws SQLException {
        connection.close();
    }
    public EntityManager getEntityManager() {
        return em;
    }

    public void setEntityManager(EntityManager em) {
        this.em = em;
    }
    
    public void closeEntityManager() throws SQLException {
        em.close();
    }
}
