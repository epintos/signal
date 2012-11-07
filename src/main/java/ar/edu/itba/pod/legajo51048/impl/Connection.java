package ar.edu.itba.pod.legajo51048.impl;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.collections.CollectionUtils;
import org.apache.log4j.Logger;
import org.jgroups.Address;
import org.jgroups.JChannel;
import org.jgroups.Message;
import org.jgroups.ReceiverAdapter;
import org.jgroups.View;

/**
 * Class that represents a channel connection with Jgroup.
 * 
 * @author Esteban G. Pintos
 * 
 */
public class Connection extends ReceiverAdapter {
	private JChannel channel;
	private String clusterName = null;

	// Users in the channel
	private Set<Address> users = new HashSet<Address>();
	private MultithreadedSignalProcessor processor;
	private Logger logger;

	public Connection(String clusterName, MultithreadedSignalProcessor processor) {
		this.logger = Logger.getLogger("Connection");
		this.processor = processor;
		try {
			this.channel = new JChannel();
			this.clusterName = clusterName;
			this.connect();
		} catch (Exception e) {
		}
	}

	private void connect() {
		try {
			channel.connect(clusterName);
			channel.setReceiver(this);

			// Notifiy all that i'm ready to receive orders.
			broadcastMessage(new SignalMessage(getMyAddress(),
					SignalMessageType.IM_READY));
			logger.debug("Node connected to '" + clusterName);
			users.addAll(getMembers());
		} catch (Exception e) {
		}
	}

	public void broadcastMessage(Object obj) {
		this.sendMessageTo(null, obj);
	}

	/**
	 * Sends a message to an address
	 * 
	 * @param address
	 *            Destination address
	 * @param obj
	 *            Object being send
	 */
	public void sendMessageTo(Address address, Object obj) {
		try {
			this.channel.send(new Message(address, obj));
		} catch (Exception e) {
		}
	}

	@Override
	public void viewAccepted(View view) {
		if (users.size() > view.getMembers().size()) {
			searchFallenNode(view.getMembers());
		} else {
			searchNewNode(view.getMembers());
		}
	}

	/**
	 * Search for fallen nodes and notify the node of this.
	 * 
	 * @param members
	 *            Members in the cluster
	 */
	@SuppressWarnings("unchecked")
	private void searchFallenNode(List<Address> members) {
		Collection<Address> disjunction = CollectionUtils.disjunction(users,
				members);
		for (Address address : disjunction) {
			processor.addNotification(new SignalMessage(address,
					SignalMessageType.BYE_NODE));
		}
		users.removeAll(disjunction);
	}

	/**
	 * Search for new nodes and adds them to the users list.
	 * 
	 * @param members
	 *            Members in the cluster
	 */
	@SuppressWarnings("unchecked")
	private void searchNewNode(List<Address> members) {
		Collection<Address> disjunction = CollectionUtils.disjunction(members,
				users);
		users.addAll(disjunction);
	}

	@Override
	public void receive(Message msg) {
		SignalMessage message = (SignalMessage) msg.getObject();
		Address myAddress = getMyAddress();
		switch (((SignalMessage) msg.getObject()).getType()) {
		case SignalMessageType.ADD_SIGNAL:
			processor.addSignal(message.getSignal());
			break;
		case SignalMessageType.ADD_BACK_UP:
			processor.addBackup(message.getBackup());
			break;
		case SignalMessageType.ADD_SIGNALS:
			processor.addSignals(msg.getSrc(), message.getSignals());
			break;
		case SignalMessageType.ADD_BACK_UPS:
			processor.addBackups(message.getAddress(), message.getBackupList());
			break;
		case SignalMessageType.CHANGE_SIGNALS_OWNER:
			if (!myAddress.equals(message.getAddress())) {
				processor.changeSignalsOwner(msg.getSrc(),
						message.getAddress(), message.getSignals());
			}
			break;

		/** For acknowledges **/
		case SignalMessageType.FIND_SIMILAR_RESULT:
			processor.addAcknowledge(message);
			break;
		case SignalMessageType.FINISHED_FALLEN_NODE_REDISTRIBUTION:
			if (!msg.getSrc().equals(myAddress)) {
				processor.addAcknowledge(message);
			}
			break;
		case SignalMessageType.READY_FOR_FALLEN_NODE_REDISTRIBUTION:
			if (!msg.getSrc().equals(getMyAddress())) {
				processor.addAcknowledge(message);
			}
			break;
		case SignalMessageType.FINISHED_NEW_NODE_REDISTRIBUTION:
			if (!msg.getSrc().equals(getMyAddress())) {
				processor.addAcknowledge(message);
			}
			break;
		case SignalMessageType.ADD_BACKUPS_ACK:
			processor.addAcknowledge(message);
			break;
		case SignalMessageType.ADD_SIGNALS_ACK:
			processor.addAcknowledge(message);
			break;

		/** For notifications **/
		case SignalMessageType.FIND_SIMILAR:
			if (!myAddress.equals(msg.getSrc())) {
				processor.addNotification(message);
			}
			break;
		case SignalMessageType.IM_READY:
			if (!message.getAddress().equals(myAddress)) {
				processor.addNotification(new SignalMessage(message
						.getAddress(), SignalMessageType.NEW_NODE));
			}
			break;
		default:
			processor.addNotification(message);
			break;
		}
	}

	/**
	 * Disconnects channel
	 */
	public void disconnect() {
		channel.disconnect();
	}

	/**
	 * Close channel
	 */
	public void close() {
		channel.close();
	}

	/**
	 * Get cluster name
	 * 
	 * @return Cluster name
	 */
	public String getClusterName() {
		return this.clusterName;
	}

	/**
	 * Get members of the cluster
	 * 
	 * @return List containing members of the cluster
	 */
	public List<Address> getMembers() {
		return channel.getView().getMembers();
	}

	/**
	 * Get members quantity in the cluster
	 * 
	 * @return Members quantity
	 */
	public int getMembersQty() {
		return getMembers().size();
	}

	/**
	 * Get my address in the cluster
	 * 
	 * @return My address
	 */
	public Address getMyAddress() {
		return channel.getAddress();
	}

}
