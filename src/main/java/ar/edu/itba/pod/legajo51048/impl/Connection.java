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

import ar.edu.itba.pod.api.Result;

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
			users.add(channel.getAddress());
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
		System.out.println(view);
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
			processor.distributeBackups(address);
		}
		users.removeAll(disjunction);
	}

	@SuppressWarnings("unchecked")
	private void searchNewNode(List<Address> newMembers) {
		Collection<Address> disjunction = CollectionUtils.disjunction(
				newMembers, users);
		processor.distributeSignals();
		users.addAll(disjunction);
	}

	@Override
	public void receive(Message msg) {
		if (msg.getObject() instanceof SignalMessage) {
			SignalMessage message = (SignalMessage) msg.getObject();
			if (message.isBackup()) {
				processor.addBackup(message.getAddress(), message.getSignal());
			} else if (message.isBroadcastFind()) {
				processor.findMySimilars(msg.getSrc(), message.getSignal());
			} else {
				processor.addSignal(message.getSignal());
			}
		} else if (msg.getObject() instanceof String) {
			String str = (String) msg.getObject();
			switch (str) {
			case MultithreadedSignalProcessor.EXIT_MESSAGE:
				processor.removeBackups(msg.getSrc());
				break;
			}
		} else if (msg.getObject() instanceof Result) {
			processor.addResult((Result)msg.getObject());
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
}
