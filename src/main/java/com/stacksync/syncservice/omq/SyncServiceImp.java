package com.stacksync.syncservice.omq;

import java.util.List;
import java.util.UUID;

import omq.common.broker.Broker;
import omq.common.util.ParameterQueue;
import omq.exception.RemoteException;
import omq.server.RemoteObject;

import org.apache.log4j.Logger;

import com.stacksync.commons.models.AccountInfo;
import com.stacksync.commons.models.CommitInfo;
import com.stacksync.commons.models.Device;
import com.stacksync.commons.models.Item;
import com.stacksync.commons.models.ItemMetadata;
import com.stacksync.commons.models.User;
import com.stacksync.commons.models.Workspace;
import com.stacksync.commons.notifications.CommitNotification;
import com.stacksync.commons.notifications.ShareProposalNotification;
import com.stacksync.commons.notifications.UpdateWorkspaceNotification;
import com.stacksync.commons.omq.ISyncService;
import com.stacksync.commons.omq.RemoteClient;
import com.stacksync.commons.omq.RemoteWorkspace;
import com.stacksync.commons.requests.CommitRequest;
import com.stacksync.commons.requests.GetAccountRequest;
import com.stacksync.commons.requests.GetChangesRequest;
import com.stacksync.commons.requests.GetWorkspacesRequest;
import com.stacksync.commons.requests.ShareProposalRequest;
import com.stacksync.commons.requests.UpdateDeviceRequest;
import com.stacksync.commons.requests.UpdateWorkspaceRequest;
import com.stacksync.syncservice.db.ConnectionPool;
import com.stacksync.commons.exceptions.DeviceNotUpdatedException;
import com.stacksync.commons.exceptions.DeviceNotValidException;
import com.stacksync.commons.exceptions.NoWorkspacesFoundException;
import com.stacksync.commons.exceptions.ShareProposalNotCreatedException;
import com.stacksync.commons.exceptions.UserNotFoundException;
import com.stacksync.commons.exceptions.WorkspaceNotUpdatedException;
import com.stacksync.syncservice.exceptions.dao.DAOException;
import com.stacksync.syncservice.handler.SQLSyncHandler;
import com.stacksync.syncservice.handler.SyncHandler;
import com.stacksync.syncservice.util.Config;
import java.util.logging.Level;
import javax.persistence.EntityManagerFactory;

public class SyncServiceImp extends RemoteObject implements ISyncService {

	private transient static final Logger logger = Logger.getLogger(SyncServiceImp.class.getName());
	private transient static final long serialVersionUID = 1L;
	private transient SyncHandler handler;
	private transient Broker broker;

        public SyncServiceImp(Broker broker, EntityManagerFactory pool) throws Exception {
		super();
		this.broker = broker;
		handler = new SQLSyncHandler(pool);
	}
                
	public SyncServiceImp(Broker broker, ConnectionPool pool) throws Exception {
		super();
		this.broker = broker;
		handler = new SQLSyncHandler(pool);
	}

	@Override
	public List<ItemMetadata> getChanges(GetChangesRequest request) {

		logger.debug(request);

		User user = new User();
		user.setId(request.getUserId());
		Workspace workspace = new Workspace(request.getWorkspaceId());

		List<ItemMetadata> list = handler.doGetChanges(user, workspace);

		return list;
	}

	@Override
	public List<Workspace> getWorkspaces(GetWorkspacesRequest request) throws NoWorkspacesFoundException {
		logger.debug(request.toString());

		User user = new User();
		user.setId(request.getUserId());

		List<Workspace> workspaces = handler.doGetWorkspaces(user);

		return workspaces;
	}

	@Override
	public void commit(CommitRequest request) {
		logger.debug(request);

		try {

			User user = new User();
			user.setId(request.getUserId());
			Device device = new Device(request.getDeviceId());
			Workspace workspace = new Workspace(request.getWorkspaceId());

                        //Testing pourposes
                        //long init = System.currentTimeMillis();
            
			CommitNotification result = handler.doCommit(user, workspace, device, request.getItems());
                        result.setRequestId(request.getRequestId());
                        
                        //Testing pourposes
                        //logger.info("RequestId= "+request.getRequestId() + " - TotalTime: "+ Long.toString(System.currentTimeMillis()-init));
            
			UUID id = workspace.getId();

                        /* Commented for testing pourposes
			RemoteWorkspace commitNotifier = broker.lookupMulti(id.toString(), RemoteWorkspace.class);
			commitNotifier.notifyCommit(result);
                        */

			logger.debug("Consumer: Response sent to workspace \"" + workspace + "\"");

		} catch (DAOException e) {
			logger.error(e);
		}/* catch (RemoteException e) {
                        logger.error(e);
            }*/
	}

	@Override
	public UUID updateDevice(UpdateDeviceRequest request) throws UserNotFoundException, DeviceNotValidException,
			DeviceNotUpdatedException {

		logger.debug(request.toString());

		User user = new User();
		user.setId(request.getUserId());

		Device device = new Device();
		device.setId(request.getDeviceId());
		device.setUser(user);
		device.setName(request.getDeviceName());
		device.setOs(request.getOs());
		device.setLastIp(request.getIp());
		device.setAppVersion(request.getAppVersion());

		UUID deviceId = handler.doUpdateDevice(device);

		return deviceId;
	}

	@Override
	public void createShareProposal(ShareProposalRequest request) throws ShareProposalNotCreatedException,
			UserNotFoundException {

            try{
		logger.debug(request);

		User user = new User();
		user.setId(request.getUserId());
		
		Item item = new Item(request.getItemId());

		// Create share proposal
		Workspace workspace = handler.doShareFolder(user, request.getEmails(), item, request.isEncrypted());

		// Create notification
		ShareProposalNotification notification = new ShareProposalNotification(workspace.getId(),
				workspace.getName(), item.getId(), workspace.getOwner().getId(), workspace.getOwner().getName(),
				workspace.getSwiftContainer(), workspace.getSwiftUrl(), workspace.isEncrypted());

		notification.setRequestId(request.getRequestId());

		// Send notification to owner
		RemoteClient client;
		try {
			client = broker.lookupMulti(user.getId().toString(), RemoteClient.class);
			client.notifyShareProposal(notification);
		} catch (RemoteException e1) {
			logger.error(String.format("Could not notify user: '%s'", user.getId()), e1);
		}

		// Send notifications to users
		for (User addressee : workspace.getUsers()) {
			try {
				client = broker.lookupMulti(addressee.getId().toString(), RemoteClient.class);
				client.notifyShareProposal(notification);
			} catch (RemoteException e) {
				logger.error(String.format("Could not notify user: '%s'", addressee.getId()), e);
			}
		}
            } catch (DAOException e) {
                logger.error(e);
            }
	}

	@Override
	public void updateWorkspace(UpdateWorkspaceRequest request) throws UserNotFoundException,
			WorkspaceNotUpdatedException {
		logger.debug(request);

		User user = new User();
		user.setId(request.getUserId());
		Item item = new Item(request.getParentItemId());

		Workspace workspace = new Workspace(request.getWorkspaceId());
		workspace.setName(request.getWorkspaceName());
		workspace.setParentItem(item);

		handler.doUpdateWorkspace(user, workspace);

		// Create notification
		UpdateWorkspaceNotification notification = new UpdateWorkspaceNotification(workspace.getId(),
				workspace.getName(), workspace.getParentItem().getId());
		notification.setRequestId(request.getRequestId());

		// Send notification to owner
		RemoteClient client;
		try {
			client = broker.lookupMulti(user.getId().toString(), RemoteClient.class);
			client.notifyUpdateWorkspace(notification);
		} catch (RemoteException e1) {
			logger.error(String.format("Could not notify user: '%s'", user.getId()), e1);
		}
	}

	@Override
	public AccountInfo getAccountInfo(GetAccountRequest request) throws UserNotFoundException {
		logger.debug(request);

		User user = handler.doGetUser(request.getEmail());

		AccountInfo accountInfo = new AccountInfo();

		accountInfo.setUserId(user.getId());
		accountInfo.setName(user.getName());
		accountInfo.setEmail(user.getEmail());
		accountInfo.setQuotaLimit(user.getQuotaLimit());
		//TODO: Add real quota used to account info? 
		accountInfo.setQuotaUsed(user.getQuotaUsedLogical());
		accountInfo.setSwiftUser(user.getSwiftUser());
		accountInfo.setSwiftTenant(Config.getSwiftTenant());
		accountInfo.setSwiftAuthUrl(Config.getSwiftAuthUrl());

		return accountInfo;
	}

        /* Testing pourposes
        @Override
        public void createUser(UUID id) {
	    handler.doCreateUser(id);
        }
        */
        
}
