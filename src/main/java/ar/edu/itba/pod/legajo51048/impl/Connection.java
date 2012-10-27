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
			broadcastMessage(new SignalMessage(getMyAddress(),SignalMessageType.IM_READY));
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
		switch (((SignalMessage) msg.getObject()).getType()) {
		case SignalMessageType.YOUR_SIGNAL:
			processor.addSignal(msg.getSrc(), message.getSignal());
			break;
		case SignalMessageType.YOUR_SIGNALS:
			processor.addSignals(msg.getSrc(), message.getSignals(),
					SignalMessageType.YOUR_SIGNALS);
			break;
		case SignalMessageType.SIGNAL_REDISTRIBUTION:
			processor.addSignals(msg.getSrc(), message.getSignals(),
					SignalMessageType.SIGNAL_REDISTRIBUTION);
			break;
		case SignalMessageType.BACK_UP:
			processor.addBackup(msg.getSrc(), message.getBackup());
			break;
		case SignalMessageType.BACK_UPS:
			processor.addBackups(msg.getSrc(), message.getBackupList());
			break;
		case SignalMessageType.CHANGE_BACK_UP_OWNER:
			processor.changeBackupOwner(message.getAddress(),
					message.getSignals());
			break;
		case SignalMessageType.FIND_SIMILAR:
			processor.findMySimilars(msg.getSrc(), message.getSignal(),
					message.getRequestId());
			break;
		case SignalMessageType.BYE_NODE:
			processor.removeBackups(msg.getSrc());
			break;
		case SignalMessageType.ADD_BACKUP_OWNER:
			processor.changeWhoBackupMySignal(message.getAddress(),message.getSignal());
			break;
		case SignalMessageType.IM_READY:
			processor.addNotification(new SignalMessage(message.getAddress(),
					SignalMessageType.NEW_NODE));
			break;

		/** For notifications **/
		default:
			processor.addNotification(message);
			break;
		}
	}

	public void disconnect() {
		channel.disconnect();
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

	public Set<Address> getUsers() {
		return users;
	}

}
