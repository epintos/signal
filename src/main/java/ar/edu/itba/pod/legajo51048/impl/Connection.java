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
	private Set<Address> users = new HashSet<Address>();;

	public Connection(String clusterName) {
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
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void broadcastMessage(String msg) {
		this.sendMessageTo(null, msg);
	}

	public void sendMessageTo(Address addr, String msg) {
		try {
			this.channel.send(new Message(addr, msg));
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void doNotReceiveOwnMessages() {
		channel.setDiscardOwnMessages(true);
	}

	@Override
	public void viewAccepted(View view) {
		if (users.size() > view.getMembers().size()) {
			searchFallenNode(view.getMembers());
		} else {
			searchNewNode(view.getMembers());
		}
	}

	private void searchFallenNode(List<Address> newMembers) {
		Collection<Address> disjunction = CollectionUtils.disjunction(users,
				newMembers);
		users.removeAll(disjunction);
	}

	private void searchNewNode(List<Address> newMembers) {
		Collection<Address> disjunction = CollectionUtils.disjunction(
				newMembers, users);
		users.addAll(disjunction);
	}

	@Override
	public void receive(Message msg) {
		// TODO Auto-generated method stub
		super.receive(msg);
	}

	public void disconnect() {
		channel.disconnect();
	}

	public String getClusterName() {
		return this.clusterName;
	}
}
