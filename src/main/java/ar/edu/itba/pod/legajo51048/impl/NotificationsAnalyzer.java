package ar.edu.itba.pod.legajo51048.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
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
	private final Multimap<Address, Signal> sendSignals;
	private final MultithreadedSignalProcessor processor;
	private Connection connection;
	private final Multimap<Address, Signal> mySignalsBackup;
	private final Multimap<Address, Backup> sendBackups;
	private final Multimap<Address, Signal> sendChangeWhoBackup;
	private final ConcurrentMap<Integer, FindRequest> requests;
	private final BlockingQueue<Signal> signals;
	private final ConcurrentMap<Address, Semaphore> tasksDone;
	private final ConcurrentMap<Address, Semaphore> semaphores;

	public NotificationsAnalyzer(BlockingQueue<Signal> signals,
			BlockingQueue<SignalMessage> notifications,
			Multimap<Address, Signal> sendSignals,
			MultithreadedSignalProcessor processor,
			Multimap<Address, Signal> mySignalsBackup,
			Multimap<Address, Backup> sendBackups,
			ConcurrentMap<Integer, FindRequest> requests,
			Multimap<Address, Signal> sendChangeWhoBackup,
			ConcurrentMap<Address, Semaphore> tasksDone, Connection connection) {
		this.signals = signals;
		this.notifications = notifications;
		this.sendSignals = sendSignals;
		this.sendChangeWhoBackup = sendChangeWhoBackup;
		this.processor = processor;
		this.mySignalsBackup = mySignalsBackup;
		this.sendBackups = sendBackups;
		this.requests = requests;
		this.connection = connection;
		this.tasksDone = tasksDone;
		this.semaphores = new ConcurrentHashMap<Address, Semaphore>();
	}

	public void finish() {
		finishedAnalyzer.set(true);
	}

	@Override
	public void run() {

		while (!finishedAnalyzer.get()) {
			try {
				final SignalMessage notification;
				notification = notifications.take();
				switch (notification.getType()) {
				case SignalMessageType.REQUEST_NOTIFICATION:
					FindRequest request = requests.get(notification
							.getRequestId());
					request.addResult(notification.getResult(),
							notification.getAddress());
					break;
				case SignalMessageType.ADD_SIGNAL_ACK:
					if (!sendSignals.remove(notification.getAddress(),
							notification.getSignal())) {
						System.out.println("esto no deberia pasar "
								+ SignalMessageType.ADD_SIGNAL_ACK);
					}
					processor.distributeBackup(notification.getAddress(), null,
							notification.getSignal());
					break;
				case SignalMessageType.ADD_SIGNALS_ACK:
					// synchronized (sendSignals) {
					if (!sendSignals.get(notification.getAddress()).removeAll(
							notification.getSignals())) {
						System.out.println("esto no deberia pasar "
								+ SignalMessageType.ADD_SIGNALS_ACK);
						System.out.println("de donde vino: "
								+ notification.getAddress());
					}
					// }
					// Tell everyone that some backup owners have changed
					Address signalOwner = notification.getAddress();
					connection.broadcastMessage(new SignalMessage(signalOwner,
							notification.getSignals(),
							SignalMessageType.CHANGE_BACK_UP_OWNER));
					break;
				case SignalMessageType.GENERATE_NEW_SIGNALS_FROM_BACKUP_ACK:
					// synchronized (sendSignals) {
					for (Signal s : notification.getSignals()) {
						if (!sendSignals.remove(notification.getAddress(), s)) {
							System.out
									.println("esto no deberia pasar "
											+ SignalMessageType.GENERATE_NEW_SIGNALS_FROM_BACKUP_ACK);
							System.out.println("de donde vino: "
									+ notification.getAddress());
						}
						processor.distributeBackup(notification.getAddress(),
								notification.getOtherAddress(), s);
					}
					// }
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
						if (notification.getOtherAddress() != null) {
							Semaphore sem = tasksDone.get(notification
									.getOtherAddress());
							if (sem != null) {
								sem.release();
							}
						}
					} else {
						if (!this.sendChangeWhoBackup.put(signalOwner,
								notification.getBackup().getSignal())) {
							System.out
									.println("no deberia pasar ADD_BACKUP_ACK, put false ");
						}
						connection
								.sendMessageTo(
										signalOwner,
										new SignalMessage(
												notification.getAddress(),
												notification.getOtherAddress(),
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
							// to myself
							processor.changeWhoBackupMySignal(null,
									notification.getAddress(), null,
									b.getSignal(), true);
							if (notification.getOtherAddress() != null) {
								Semaphore sem = tasksDone.get(notification
										.getOtherAddress());
								if (sem != null) {
									sem.release();
								}
							}
						} else {
							sendChangeWhoBackup.put(b.getAddress(),
									b.getSignal());
							connection
									.sendMessageTo(
											b.getAddress(),
											new SignalMessage(
													notification.getAddress(),
													notification
															.getOtherAddress(),
													b.getSignal(),
													SignalMessageType.CHANGE_WHO_BACK_UP_MYSIGNAL));
						}
					}
					break;
				case SignalMessageType.CHANGE_WHO_BACK_UP_MYSIGNAL_ACK:
					if (!sendChangeWhoBackup.remove(notification.getAddress(),
							notification.getSignal())) {
						System.out
								.println("no deberia pasar "
										+ SignalMessageType.CHANGE_WHO_BACK_UP_MYSIGNAL_ACK);
						System.out.println("de donde vino: "
								+ notification.getAddress());
					}
					if (notification.getOtherAddress() != null) {
						Semaphore sem = tasksDone.get(notification
								.getOtherAddress());
						if (sem != null) {
							sem.release();
						}
					}
					break;
				case SignalMessageType.FINISHED_REDISTRIBUTION:
					Semaphore semaphore = semaphores.get(notification
							.getOtherAddress());
					if (semaphore == null) {
						semaphore = new Semaphore(0);
						semaphores.put(notification.getOtherAddress(),
								semaphore);
					}
					semaphore.release();
					break;
				case SignalMessageType.NEW_NODE:
					processor.distributeSignals(notification.getAddress());
					break;
				case SignalMessageType.BYE_NODE:

					new Thread() {
						public void run() {
							try {
								Address fallenNodeAddress = notification
										.getAddress();
								int tasks = processor
										.distributeNewSignalsFromBackups(notification
												.getAddress());
								Semaphore sem = tasksDone
										.get(fallenNodeAddress);
								while (!sem.tryAcquire(tasks, 1,
										TimeUnit.SECONDS)) {
									System.out.println("en el 1er while "
											+ sem.availablePermits());
								}
								tasks = processor
										.distributeLostBackups(notification
												.getAddress());
								sem = tasksDone.get(fallenNodeAddress);
								while (!sem.tryAcquire(tasks, 1,
										TimeUnit.SECONDS)) {
									System.out.println("en el 2do while "
											+ sem.availablePermits());
								}
								tasksDone.remove(fallenNodeAddress);
								System.out.println("finished distributing");
								connection
										.broadcastMessage(new SignalMessage(
												connection.getMyAddress(),
												fallenNodeAddress,
												SignalMessageType.FINISHED_REDISTRIBUTION));

								// Wait for all the nodes to distribute
								try {
									Semaphore semaphore = semaphores
											.get(fallenNodeAddress);
									if (semaphore == null) {
										semaphore = new Semaphore(0);
										semaphores.put(fallenNodeAddress,
												semaphore);
									}
									while (!semaphore.tryAcquire(
											connection.getMembersQty() - 1,
											1000, TimeUnit.MILLISECONDS)) {
										System.out
												.println("while de FINISHED_REDISTRIBUTION: "
														+ semaphore
																.availablePermits());
									}
								} catch (InterruptedException e) {
									e.printStackTrace();
								}
								semaphores.remove(fallenNodeAddress);
								System.out.println("finished recovery");

								// Find find similar request that had the
								// fallen
								// node
								for (FindRequest request : requests.values()) {
									if (request.getAddresses().contains(
											fallenNodeAddress)) {
										List<Address> addresses = new ArrayList<Address>(
												connection.getMembers());
										request.restart(addresses);
										connection
												.broadcastMessage(new SignalMessage(
														request.getSignal(),
														request.getRequestId(),
														SignalMessageType.FIND_SIMILAR));

									}
								}

							} catch (InterruptedException e) {
								e.printStackTrace();
							}
						}
					}.start();

					break;
				}
			} catch (InterruptedException e) {
			}
		}
	}
}
