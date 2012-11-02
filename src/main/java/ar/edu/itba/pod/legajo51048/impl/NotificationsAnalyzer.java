package ar.edu.itba.pod.legajo51048.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
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
	private final BlockingQueue<Signal> sendSignals;
	private final MultithreadedSignalProcessor processor;
	private final Multimap<Address, Signal> backups;
	private final BlockingQueue<Backup> sendBackups;
	private final ConcurrentMap<Integer, FindRequest> requests;
	private final BlockingQueue<Signal> signals;
	private Connection connection;
	private Semaphore semaphore = new Semaphore(0);

	public NotificationsAnalyzer(BlockingQueue<Signal> signals,
			BlockingQueue<SignalMessage> notifications,
			BlockingQueue<Signal> sendSignals,
			MultithreadedSignalProcessor processor,
			BlockingQueue<Backup> sendBackups,
			ConcurrentMap<Integer, FindRequest> requests,

			Multimap<Address, Signal> backups, Connection connection) {
		this.signals = signals;
		this.notifications = notifications;
		this.sendSignals = sendSignals;
		this.processor = processor;
		this.sendBackups = sendBackups;
		this.requests = requests;
		this.connection = connection;
		this.backups = backups;
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
					FindRequest request = requests.get(notification
							.getRequestId());
					request.addResult(notification.getResult(),
							notification.getAddress());
					break;

				case SignalMessageType.ADD_SIGNAL_ACK:
					if (!sendSignals.remove(notification.getSignal())) {
						System.out.println("esto no deberia pasar "
								+ SignalMessageType.ADD_SIGNAL_ACK);
					}
					processor.distributeBackup(notification.getAddress(),
							notification.getSignal());
					break;

				case SignalMessageType.ADD_SIGNALS_ACK:
					if (!sendSignals.removeAll(notification.getSignals())) {
						System.out.println("esto no deberia pasar "
								+ SignalMessageType.ADD_SIGNALS_ACK);
					}
					// Tell everyone that some backup owners have changed
					Address signalOwner = notification.getAddress();
					connection.broadcastMessage(new SignalMessage(signalOwner,
							notification.getSignals(),
							SignalMessageType.CHANGE_BACK_UP_OWNER));
					break;

				case SignalMessageType.GENERATE_NEW_SIGNALS_FROM_BACKUP_ACK:
					for (Signal s : notification.getSignals()) {
						if (!sendSignals.remove(s)) {
							System.out
									.println("esto no deberia pasar "
											+ SignalMessageType.GENERATE_NEW_SIGNALS_FROM_BACKUP_ACK);
						}
						processor
								.distributeBackup(notification.getAddress(), s);
					}
					break;

				case SignalMessageType.ADD_BACKUP_ACK:
					if (!sendBackups.remove(notification.getBackup())) {
						System.out.println("no deberia pasar "
								+ SignalMessageType.ADD_BACKUP_ACK);
					}

					break;

				case SignalMessageType.ADD_BACKUPS_ACK:
					if (!sendBackups.removeAll(notification.getBackupList())) {
						System.out.println("no deberia pasar "
								+ SignalMessageType.ADD_BACKUPS_ACK);
					}
					break;

				case SignalMessageType.NEW_NODE:
					processor.distributeSignals(notification.getAddress());
					break;

				case SignalMessageType.FINISHED_REDISTRIBUTION:
					System.out.println("llega FINISHED_REDISTRIBUTION ");
					semaphore.release();
					break;

				case SignalMessageType.BYE_NODE:

					Address fallenNodeAddress = notification.getAddress();

					// Signalibute lost signals.
					BlockingQueue<Signal> toDistribute = new LinkedBlockingDeque<Signal>();
					toDistribute.addAll(this.signals);
					System.out.println("signals size:" + this.signals.size());
					System.out.println("backups size:"
							+ this.backups.get(fallenNodeAddress).size());
					toDistribute.addAll(backups.get(fallenNodeAddress));
					this.signals.clear();
					this.backups.clear();
					System.out.println("to distribute: " + toDistribute.size());
					processor.distributeSignals(toDistribute);

					System.out.println("finished distributing");
					// Tell everyone that distribution finished.
					connection.broadcastMessage(new SignalMessage(connection
							.getMyAddress(), fallenNodeAddress,
							SignalMessageType.FINISHED_REDISTRIBUTION));

					// Wait for all the nodes to distribute
					while (!semaphore.tryAcquire(
							connection.getMembersQty() - 1, 1000,
							TimeUnit.MILLISECONDS)) {
						System.out.println("while de FINISHED_REDISTRIBUTION: "
								+ semaphore.availablePermits());
					}
					semaphore = new Semaphore(0);
					System.out.println("finished recovery");

					// Find requests that had aborted cause the node fell
					for (FindRequest r : requests.values()) {
						if (r.getAddresses().contains(fallenNodeAddress)) {
							List<Address> addresses = new ArrayList<Address>(
									connection.getMembers());
							r.restart(addresses);
							connection.broadcastMessage(new SignalMessage(r
									.getSignal(), r.getRequestId(),
									SignalMessageType.FIND_SIMILAR));

						}
					}

					break;
				}
			} catch (InterruptedException e) {
			}
		}
	}

}
