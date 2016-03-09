package com.stacksync.syncservice.db.hibernateOGM;

import java.util.UUID;

import org.apache.log4j.Logger;

import com.stacksync.commons.models.Device;
import com.stacksync.syncservice.db.DAOPersistenceContext;
import com.stacksync.syncservice.db.DeviceDAO;
import com.stacksync.syncservice.exceptions.dao.DAOException;
import com.stacksync.syncservice.util.Constants;
import java.util.Calendar;
import java.util.Date;

public class HibernateOGMDeviceDAO extends HibernateOGMDAO implements DeviceDAO {

    private static final Logger logger = Logger.getLogger(HibernateOGMDeviceDAO.class.getName());

    public HibernateOGMDeviceDAO() {
    }

    @Override
    public Device get(UUID deviceID, DAOPersistenceContext persistenceContext){
        // API device ID is not stored in the database
        if (deviceID == Constants.API_DEVICE_ID) {
            return new Device(Constants.API_DEVICE_ID);
        }

        return (Device) persistenceContext.getEntityManager().find(Device.class, deviceID);
    }

    @Override
    public void add(Device device,DAOPersistenceContext persistenceContext){
        if (!device.isValid()) {
            throw new IllegalArgumentException("Device attributes not set");
        }
        
        persistenceContext.getEntityManager().persist(device);
   
    }

    @Override
    public void update(Device device,DAOPersistenceContext persistenceContext){
        if (device.getId() == null || !device.isValid()) {
            throw new IllegalArgumentException("Device attributes not set");
        }

        Device deviceDB = (Device) persistenceContext.getEntityManager().find(Device.class, device);

        Calendar calendar = Calendar.getInstance();
        Date now = calendar.getTime();

        deviceDB.setLastAccessAt(now);
        deviceDB.setLastIp(device.getLastIp());
        deviceDB.setAppVersion(device.getAppVersion());

        persistenceContext.getEntityManager().merge(deviceDB);

    }

    @Override
    public void delete(UUID deviceID,DAOPersistenceContext persistenceContext) throws DAOException {
        Device deviceDB = (Device) persistenceContext.getEntityManager().find(Device.class, deviceID);
        persistenceContext.getEntityManager().remove(deviceDB);
    }
}
