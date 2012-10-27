package ar.edu.itba.pod.legajo51048.impl;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import org.jgroups.Address;

import ar.edu.itba.pod.api.Signal;

import com.google.common.collect.Multimap;

/**
 * Worker that analyzes the notifications of a node
 * 
 * @author Esteban G. Pintos
 * 
 */
public class NotificationsAnalyzer extends Thread {
	private AtomicBoolean finishedAnalyzer = new AtomicBoolean(false);
	private final BlockingQueue<SignalMessage> notifications;
	private final Multimap<Address, Signal> sendSignals;
	private final MultithreadedSignalProcessor processor;
	private Connection connection;
	private final Multimap<Address, Signal> mySignalsBackup;
	private final Multimap<Address, Backup> sendBackups;
	private final List<FindRequest> requests;
	private final BlockingQueue<Signal> signals;

	public NotificationsAnalyzer(BlockingQueue<Signal> signals,
			BlockingQueue<SignalMessage> notifications,
			Multimap<Address, Signal> sendSignals,
			MultithreadedSignalProcessor processor,
			Multimap<Address, Signal> mySignalsBackup,
			Multimap<Address, Backup> sendBackups, List<FindRequest> requests) {
		this.signals = signals;
		this.notifications = notifications;
		this.sendSignals = sendSignals;
		this.processor = processor;
		this.mySignalsBackup = mySignalsBackup;
		this.sendBackups = sendBackups;
		this.requests = requests;
	}

	public void setConnection(Connection connection) {
		this.connection = connection;
	}

	@Override
	public void run() {
		while (!finishedAnalyzer.get()) {
			try {
				SignalMessage notification;
				notification = notifications.take();
				switch (notification.getType()) {
				case SignalMessageType.REQUEST_NOTIFICATION:
					for (FindRequest request : requests) {
						if (request.getId() == notification.getRequestId()) {
							request.removeAddress(notification.getAddress());
							request.addResult(notification.getResult());
							request.getSemaphore().release();
							break;
						}
					}
					break;
				case SignalMessageType.ADD_SIGNAL_ACK:
					sendSignals.remove(notification.getAddress(),
							notification.getSignal());
					processor.distributeBackup(notification.getAddress(),
							notification.getSignal());
					break;
				case SignalMessageType.ADD_SIGNALS_ACK:
					sendSignals.remove(notification.getAddress(),
							notification.getSignals());
					connection.broadcastMessage(new SignalMessage(notification
							.getAddress(), notification.getSignals(),
							SignalMessageType.CHANGE_BACK_UP_OWNER));
					break;
				case SignalMessageType.BACKUP_REDISTRIBUTION_ACK:
					sendSignals.remove(notification.getAddress(),
							notification.getSignals());
					for (Signal signal : notification.getSignals()) {
						processor.distributeBackup(notification.getAddress(),
								signal);
					}
					break;
				case SignalMessageType.ADD_BACKUP_ACK:
					sendBackups.remove(notification.getAddress(),
							notification.getBackup());
					if (signals.contains(notification.getBackup().getSignal())) {
						mySignalsBackup.put(notification.getAddress(),
								notification.getBackup().getSignal());
					} else {
						connection.sendMessageTo(notification.getBackup()
								.getAddress(),
								new SignalMessage(notification.getAddress(),
										notification.getBackup().getSignal(),
										SignalMessageType.ADD_BACKUP_OWNER));
					}
					break;
				case SignalMessageType.ADD_BACKUPS_ACK:
					for (Backup b : notification.getBackupList()) {
						sendBackups.remove(notification.getAddress(), b);
						if (signals.contains(b.getSignal())) {
							mySignalsBackup.put(notification.getAddress(),
									b.getSignal());
						} else {
							connection
									.sendMessageTo(
											b.getAddress(),
											new SignalMessage(
													notification.getAddress(),
													b.getSignal(),
													SignalMessageType.ADD_BACKUP_OWNER));
						}
					}
					break;

				case SignalMessageType.NEW_NODE:
					processor.distributeSignals(notification.getAddress());
					break;
				case SignalMessageType.BYE_NODE:
					processor.distributeNewSignalsFromBackups(notification
							.getAddress());
					processor.distributeLostBackups(notification.getAddress());
					break;
				}
			} catch (InterruptedException e) {
			}
		}
	}

	public void finish() {
		finishedAnalyzer.set(true);
	}
}
