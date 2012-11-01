package ar.edu.itba.pod.legajo51048.impl;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.collections.CollectionUtils;
import org.jgroups.Address;
import org.jgroups.JChannel;
import org.jgroups.Message;
import org.jgroups.ReceiverAdapter;
import org.jgroups.View;

/**
 * Class that represents a channel connection with Jgroup
 * 
 * @author Esteban G. Pintos
 * 
 */
public class Connection extends ReceiverAdapter {
	private JChannel channel;
	private String clusterName = null;
	private Set<Address> users = new HashSet<Address>();
	private MultithreadedSignalProcessor processor;

	public Connection(String clusterName, MultithreadedSignalProcessor processor) {
		this.processor = processor;
		try {
			this.channel = new JChannel();
			this.clusterName = clusterName;
			this.connect();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private void connect() {
		try {
			channel.connect(clusterName);
			channel.setReceiver(this);
			// Notifiy all that i'm ready to receive orders.
			broadcastMessage(new SignalMessage(getMyAddress(),
					SignalMessageType.IM_READY));
			users.addAll(getMembers());
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void broadcastMessage(Object obj) {
		this.sendMessageTo(null, obj);
	}

	public void sendMessageTo(Address address, Object obj) {
		try {
			this.channel.send(new Message(address, obj));
		} catch (Exception e) {
			e.printStackTrace();
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

	@SuppressWarnings("unchecked")
	private void searchFallenNode(List<Address> newMembers) {
		Collection<Address> disjunction = CollectionUtils.disjunction(users,
				newMembers);
		for (Address address : disjunction) {
			processor.addNotification(new SignalMessage(address,
					SignalMessageType.BYE_NODE));
		}
		users.removeAll(disjunction);
	}

	@SuppressWarnings("unchecked")
	private void searchNewNode(List<Address> newMembers) {
		Collection<Address> disjunction = CollectionUtils.disjunction(
				newMembers, users);
		users.addAll(disjunction);
	}

	@Override
	public void receive(Message msg) {
		SignalMessage message = (SignalMessage) msg.getObject();
		Address myAddress = getMyAddress();
		switch (((SignalMessage) msg.getObject()).getType()) {
		case SignalMessageType.YOUR_SIGNAL:
			processor.addSignal(msg.getSrc(), message.getSignal());
			break;
		case SignalMessageType.YOUR_SIGNALS:
			processor.addSignals(msg.getSrc(), null, message.getSignals(),
					SignalMessageType.YOUR_SIGNALS);
			break;
		case SignalMessageType.GENERATE_NEW_SIGNALS_FROM_BACKUP:
			processor.addSignals(msg.getSrc(), message.getOtherAddress(),
					message.getSignals(),
					SignalMessageType.GENERATE_NEW_SIGNALS_FROM_BACKUP);
			break;
		case SignalMessageType.BACK_UP:
			processor.addBackup(msg.getSrc(), message.getOtherAddress(),
					message.getBackup());
			break;
		case SignalMessageType.BACK_UPS:
			processor.addBackups(msg.getSrc(), message.getOtherAddress(),
					message.getBackupList());
			break;
		case SignalMessageType.CHANGE_BACK_UP_OWNER:
			if (!myAddress.equals(message.getAddress())) {
				processor.changeBackupOwner(msg.getSrc(), message.getAddress(),
						message.getSignals());
			}
			break;
		case SignalMessageType.FIND_SIMILAR:
			if (!myAddress.equals(msg.getSrc())) {
				processor.findMySimilars(msg.getSrc(), message.getSignal(),
						message.getRequestId());
			}
			break;
		case SignalMessageType.CHANGE_WHO_BACK_UP_MYSIGNAL:
			processor.changeWhoBackupMySignal(msg.getSrc(),
					message.getAddress(), message.getOtherAddress(),
					message.getSignal(), false);
			break;
		case SignalMessageType.IM_READY:
			if (!message.getAddress().equals(getMyAddress())) {
				processor.addNotification(new SignalMessage(message
						.getAddress(), SignalMessageType.NEW_NODE));
			}
			break;
		case SignalMessageType.FINISHED_REDISTRIBUTION:
			if (!msg.getSrc().equals(getMyAddress())) {
				processor.addNotification(new SignalMessage(message
						.getAddress(),message.getOtherAddress(),
						SignalMessageType.FINISHED_REDISTRIBUTION));
			}
			break;
		// case SignalMessageType.STOP_FINDING_SIMILARS:
		// if (!message.getAddress().equals(getMyAddress())) {
		// processor
		// .addNotification(new SignalMessage(
		// message.getAddress(),
		// SignalMessageType.STOP_FINDING_SIMILARS));
		// }
		// break;

		/** For notifications **/
		default:
			processor.addNotification(message);
			break;
		}
	}

	public void disconnect() {
		channel.disconnect();
	}

	public void close() {
		channel.close();
	}

	public String getClusterName() {
		return this.clusterName;
	}

	public List<Address> getMembers() {
		return channel.getView().getMembers();
	}

	public int getMembersQty() {
		return getMembers().size();
	}

	public Address getMyAddress() {
		return channel.getAddress();
	}

}
