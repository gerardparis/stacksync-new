package com.stacksync.syncservice;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

import omq.common.broker.Broker;

import org.apache.commons.daemon.Daemon;
import org.apache.commons.daemon.DaemonContext;
import org.apache.commons.daemon.DaemonInitException;
import org.apache.log4j.Logger;

import com.stacksync.commons.omq.ISyncService;
import com.stacksync.syncservice.db.ConnectionPool;
import com.stacksync.syncservice.db.ConnectionPoolFactory;
import com.stacksync.syncservice.db.hibernateOGM.HibernateOGMEntityManagerFactory;
import com.stacksync.syncservice.exceptions.dao.DAOConfigurationException;
import com.stacksync.syncservice.omq.SyncServiceImp;
import com.stacksync.syncservice.rpc.XmlRpcSyncHandler;
import com.stacksync.syncservice.rpc.XmlRpcSyncServer;
import com.stacksync.syncservice.storage.StorageFactory;
import com.stacksync.syncservice.storage.StorageManager;
import com.stacksync.syncservice.storage.StorageManager.StorageType;
import com.stacksync.syncservice.util.Config;
import com.stacksync.syncservice.util.Constants;
import javax.persistence.EntityManagerFactory;

public class SyncServiceDaemon implements Daemon {

    private static final Logger logger = Logger
            .getLogger(SyncServiceDaemon.class.getName());
    private static ConnectionPool pool = null;
    private static EntityManagerFactory em_pool = null;
    private static XmlRpcSyncServer xmlRpcServer = null;
    private static Broker broker = null;
    private static SyncServiceImp syncService = null;

    @Override
    public void init(DaemonContext dc) throws DaemonInitException, Exception {

        logger.info(String.format("Initializing StackSync Server v%s...",
                SyncServiceDaemon.getVersion()));

        logger.info(String.format("Java VM: %s", System.getProperty("java.vm.name")));
        logger.info(String.format("Java VM version: %s", System.getProperty("java.vm.version")));
        logger.info(String.format("Java Home: %s", System.getProperty("java.home")));
        logger.info(String.format("Java version: %s", System.getProperty("java.version")));

        try {
            String[] argv = dc.getArguments();

            if (argv.length == 0) {
                logger.error("No config file passed to StackSync Server.");
                System.exit(1);
            }

            String configPath = argv[0];

            File file = new File(configPath);
            if (!file.exists()) {
                logger.error("'" + configPath + "' file not found");
                System.exit(2);
            }

            Config.loadProperties(configPath);

        } catch (IOException e) {
            logger.error("Could not load properties file.", e);
            System.exit(7);
        }

        try {

            String datasource = Config.getDatasource();
            
            if (datasource.equals("hibernateOGM")){
                em_pool = HibernateOGMEntityManagerFactory.getEntityManagerFactory();
            } else {
                pool = ConnectionPoolFactory.getConnectionPool(datasource);
                // it will try to connect to the DB, throws exception if not
                // possible.
                Connection conn = pool.getConnection();
                conn.close();
            }
            


            logger.info("Connection to database succeded");
        } catch (DAOConfigurationException e) {
            logger.error("Connection to database failed.", e);
            System.exit(3);
        } catch (SQLException e) {
            logger.error("Connection to database failed.", e);
            System.exit(4);
        }

        logger.info("Connecting to OpenStack Swift...");

        /* Commented for testing pourposes
        try {
            StorageType type;
            if (Config.getSwiftKeystoneProtocol().equals("http")) {
                type = StorageType.SWIFT;
            } else {
                type = StorageType.SWIFT_SSL;
            }
            StorageManager storageManager = StorageFactory.getStorageManager(type);
            storageManager.login();
            logger.info("Connected to OpenStack Swift successfully");
        } catch (Exception e) {
            logger.fatal("Could not connect to Swift.", e);
            System.exit(7);
        }*/


        logger.info("Initializing the messaging middleware...");
        try {
            broker = new Broker(Config.getProperties());
            if(em_pool!=null){
               syncService = new SyncServiceImp(broker, em_pool); 
            } else {
               syncService = new SyncServiceImp(broker, pool);
            }
            logger.info("Messaging middleware initialization succeeded");
        } catch (Exception e) {
            logger.error("Could not initialize ObjectMQ.", e);
            System.exit(5);
        }
    }

    @Override
    public void start() throws Exception {

        try {
            if(Config.getProperties().getProperty("omq.queueName")!=null){
                broker.bind(Config.getProperties().getProperty("omq.queueName"), syncService);
            } else {
                broker.bind(ISyncService.class.getSimpleName(), syncService);
            }
            logger.info("StackSync Server is ready and waiting for messages...");
        } catch (Exception e) {
            logger.fatal("Could not bind queue.", e);
            System.exit(5);
        }

        /* Commented for testing pourposes
        logger.info("Initializing XML RPC...");
        try {
            launchXmlRpc();
            logger.info("XML RPC initialization succeded");
        } catch (Exception e) {
            logger.fatal("Could not initialize XMLRPC.", e);
            System.exit(6);
        }*/
    }

    @Override
    public void stop() throws Exception {
        try {
            broker.stopBroker();
        } catch (Exception e) {
            logger.fatal("Error stoping StackSync Server.", e);
            throw e;
        }
    }

    @Override
    public void destroy() {
        broker = null;
    }

    private static void launchXmlRpc() throws Exception {
        xmlRpcServer = new XmlRpcSyncServer(Constants.XMLRPC_PORT);
        xmlRpcServer.addHandler("XmlRpcSyncHandler", new XmlRpcSyncHandler(
                broker, pool));
        xmlRpcServer.serve_forever();
    }

    private static String getVersion() {
        String path = "/version.properties";
        InputStream stream = Config.class.getResourceAsStream(path);
        if (stream == null) {
            return "UNKNOWN";
        }
        Properties props = new Properties();
        try {
            props.load(stream);
            stream.close();
            return (String) props.get("version");
        } catch (IOException e) {
            return "UNKNOWN";
        }
    }
}
