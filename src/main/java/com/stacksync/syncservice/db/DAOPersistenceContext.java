/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.stacksync.syncservice.db;

import java.sql.Connection;
import java.sql.SQLException;

/**
 *
 * @author marcruiz
 */
public class DAOPersistenceContext {
    
    Connection connection;

    public void beginTransaction(ConnectionPool pool) throws SQLException{
        connection = pool.getConnection();
        connection.setAutoCommit(false);
    }
    
    public void commitTransaction() throws SQLException{
        connection.commit();
        connection.setAutoCommit(true);
    }
    
    public void rollBackTransaction() throws SQLException{
        connection.rollback();
        connection.setAutoCommit(true);
    }

    public Connection getConnection() {
        return connection;
    }

    public void setConnection(Connection connection) {
        this.connection = connection;
    }
    
    
    
    
}
