package com.stacksync.syncservice.db;

import com.stacksync.syncservice.db.hibernateOGM.HibernateOGMDeviceDAO;
import com.stacksync.syncservice.db.hibernateOGM.HibernateOGMItemDAO;
import com.stacksync.syncservice.db.hibernateOGM.HibernateOGMItemVersionDao;
import com.stacksync.syncservice.db.hibernateOGM.HibernateOGMUserDAO;
import com.stacksync.syncservice.db.hibernateOGM.HibernateOGMWorkspaceDAO;

import com.stacksync.syncservice.db.postgresql.PostgresqlDeviceDAO;
import com.stacksync.syncservice.db.postgresql.PostgresqlItemDAO;
import com.stacksync.syncservice.db.postgresql.PostgresqlItemVersionDao;
import com.stacksync.syncservice.db.postgresql.PostgresqlUserDAO;
import com.stacksync.syncservice.db.postgresql.PostgresqlWorkspaceDAO;

public class DAOFactory {

    private String type;

    public DAOFactory(String type) {
        this.type = type;
    }

    public WorkspaceDAO getWorkspaceDao() {
        if ("hibernateOGM".equals(type)) {
            return new HibernateOGMWorkspaceDAO();
        } else {
            return new PostgresqlWorkspaceDAO();
        }
    }

    public UserDAO getUserDao() {
        if ("hibernateOGM".equals(type)) {
            return new HibernateOGMUserDAO();
        } else {
            return new PostgresqlUserDAO();
        }
    }

    public ItemDAO getItemDAO() {
        if ("hibernateOGM".equals(type)) {
            return new HibernateOGMItemDAO();
        } else {
            return new PostgresqlItemDAO();
        }
    }

    public ItemVersionDAO getItemVersionDAO() {
        if ("hibernateOGM".equals(type)) {
            return new HibernateOGMItemVersionDao();
        } else {
            return new PostgresqlItemVersionDao();
        }

    }

    public DeviceDAO getDeviceDAO() {
        if ("hibernateOGM".equals(type)) {
            return new HibernateOGMDeviceDAO();
        } else {
            return new PostgresqlDeviceDAO();

        }
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }
}
