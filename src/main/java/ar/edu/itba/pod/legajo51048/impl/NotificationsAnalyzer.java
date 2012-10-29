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
			Multimap<Address, Backup> sendBackups, List<FindRequest> requests,
			Connection connection) {
		this.signals = signals;
		this.notifications = notifications;
		this.sendSignals = sendSignals;
		this.processor = processor;
		this.mySignalsBackup = mySignalsBackup;
		this.sendBackups = sendBackups;
		this.requests = requests;
		this.connection = connection;
	}

	public void finish() {
		finishedAnalyzer.set(true);
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
					if (!sendSignals.remove(notification.getAddress(),
							notification.getSignal())) {
						System.out.println("esto no deberia pasar "
								+ SignalMessageType.ADD_SIGNAL_ACK);
					}
					processor.distributeBackup(notification.getAddress(),
							notification.getSignal());
					break;
				case SignalMessageType.ADD_SIGNALS_ACK:
					synchronized (sendSignals) {
						if (!sendSignals.get(notification.getAddress())
								.removeAll(notification.getSignals())) {
							System.out.println("esto no deberia pasar "
									+ SignalMessageType.ADD_SIGNALS_ACK);
							System.out.println("de donde vino: "
									+ notification.getAddress());
						}
					}
					// Tell everyone that some backup owners have changed
					Address signalOwner = notification.getAddress();
					connection.broadcastMessage(new SignalMessage(signalOwner,
							notification.getSignals(),
							SignalMessageType.CHANGE_BACK_UP_OWNER));
					break;
				case SignalMessageType.GENERATE_NEW_SIGNALS_FROM_BACKUP_ACK:
					synchronized (sendSignals) {
						for (Signal s : notification.getSignals()) {
							if (!sendSignals.remove(notification.getAddress(),
									s)) {
								System.out
										.println("esto no deberia pasar "
												+ SignalMessageType.GENERATE_NEW_SIGNALS_FROM_BACKUP_ACK);
								System.out.println("de donde vino: "
										+ notification.getAddress());
							}
							processor.distributeBackup(
									notification.getAddress(), s);
						}
					}
					break;
				case SignalMessageType.ADD_BACKUP_ACK:
					if (!sendBackups.remove(notification.getAddress(),
							notification.getBackup())) {
						System.out.println("no deberia pasar "
								+ SignalMessageType.ADD_BACKUP_ACK);
					}

					signalOwner = notification.getBackup().getAddress();
					// If I am the owner of the signal...
					if (connection.getMyAddress().equals(signalOwner)) {
						mySignalsBackup.put(notification.getAddress(),
								notification.getBackup().getSignal());
					} else {
						connection
								.sendMessageTo(
										signalOwner,
										new SignalMessage(
												notification.getAddress(),
												notification.getBackup()
														.getSignal(),
												SignalMessageType.CHANGE_WHO_BACK_UP_MYSIGNAL));
					}
					break;
				case SignalMessageType.ADD_BACKUPS_ACK:
					for (Backup b : notification.getBackupList()) {
						if (!sendBackups.remove(notification.getAddress(), b)) {
							System.out.println("no deberia pasar "
									+ SignalMessageType.ADD_BACKUPS_ACK);
						}
						if (signals.contains(b.getSignal())) {
							// mySignalsBackup.put(notification.getAddress(),
							// b.getSignal());
							processor.changeWhoBackupMySignal(
									notification.getAddress(), b.getSignal(),true);
						} else {
							connection
									.sendMessageTo(
											b.getAddress(),
											new SignalMessage(
													notification.getAddress(),
													b.getSignal(),
													SignalMessageType.CHANGE_WHO_BACK_UP_MYSIGNAL));
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
}
