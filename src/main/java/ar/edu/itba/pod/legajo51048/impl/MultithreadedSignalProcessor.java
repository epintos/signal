package ar.edu.itba.pod.legajo51048.impl;

import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.jgroups.Address;

import ar.edu.itba.pod.api.NodeStats;
import ar.edu.itba.pod.api.Result;
import ar.edu.itba.pod.api.Result.Item;
import ar.edu.itba.pod.api.SPNode;
import ar.edu.itba.pod.api.Signal;
import ar.edu.itba.pod.api.SignalProcessor;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;

public class MultithreadedSignalProcessor implements SPNode, SignalProcessor {

	private final BlockingQueue<Signal> signals;
	private final Multimap<Address, Signal> sendSignals;
	private final Multimap<Address, Signal> backups;
	private final Multimap<Address, Signal> mySignalsBackup;
	private final Multimap<Address, Backup> sendBackups;
	private final BlockingQueue<SignalMessage> notifications;
	private final List<FindRequest> requests;
	private final ExecutorService executor;
	private final int threadsQty;

	private AtomicBoolean degraded = new AtomicBoolean(false);
	private AtomicInteger receivedSignals = new AtomicInteger(0);
	private Connection connection = null;
	private NotificationsAnalyzer notificationsAnalyzer;

	public MultithreadedSignalProcessor(int threadsQty) {
		ArrayListMultimap<Address, Signal> list = ArrayListMultimap.create();
		this.backups = Multimaps.synchronizedListMultimap(list);
		ArrayListMultimap<Address, Backup> list2 = ArrayListMultimap.create();
		this.sendBackups = Multimaps.synchronizedListMultimap(list2);
		ArrayListMultimap<Address, Signal> list4 = ArrayListMultimap.create();
		this.mySignalsBackup = Multimaps.synchronizedListMultimap(list4);
		this.notifications = new LinkedBlockingQueue<SignalMessage>();
		this.requests = new ArrayList<FindRequest>();
		this.signals = new LinkedBlockingQueue<Signal>();
		ArrayListMultimap<Address, Signal> list3 = ArrayListMultimap.create();
		this.sendSignals = Multimaps.synchronizedListMultimap(list3);
		this.executor = Executors.newFixedThreadPool(threadsQty);
		this.threadsQty = threadsQty;
		this.notificationsAnalyzer = new NotificationsAnalyzer();
		this.notificationsAnalyzer.start();
	}

	@Override
	public void join(String clusterName) throws RemoteException {
		if (connection != null && connection.getClusterName() != null) {
			throw new IllegalStateException("Already in cluster "
					+ connection.getClusterName());
		}
		if (!signals.isEmpty()) {
			throw new IllegalStateException(
					"Can't join a cluster because there are signals already stored");
		}
		this.connection = new Connection(clusterName, this);

	}

	@Override
	public void exit() throws RemoteException {
		signals.clear();
		receivedSignals = new AtomicInteger(0);
		if (connection != null) {
			connection.broadcastMessage(SignalMessageType.BYE_NODE);
			connection.disconnect();
		}
		notificationsAnalyzer.finish();
	}

	@Override
	public NodeStats getStats() throws RemoteException {
		return new NodeStats("cluster " + connection.getClusterName(),
				receivedSignals.longValue(), signals.size(), backups.size(),
				true);
	}

	@Override
	public void add(Signal signal) throws RemoteException {
		distributeNewSignal(signal);
	}

	private void distributeNewSignal(Signal signal) {
		int membersQty = 1;
		List<Address> users = null;
		if (connection != null) {
			users = connection.getMembers();
			membersQty = users.size();
		}
		if (membersQty != 1) {
			int random = random(membersQty);
			Address futureOwner = users.get(random);
			Address myAddress = connection.getMyAddress();
			if (myAddress.equals(futureOwner)) {
				this.signals.add(signal);
				distributeBackup(myAddress, signal);
			} else {
				connection.sendMessageTo(futureOwner, new SignalMessage(signal,
						SignalMessageType.YOUR_SIGNAL));
				this.sendSignals.put(futureOwner, signal);
			}
		} else {
			this.signals.add(signal);
			// TODO: Ver que hacer con los backups si no hay conexión
			if (connection != null) {
				this.backups.put(connection.getMyAddress(), signal);
				this.mySignalsBackup.put(connection.getMyAddress(), signal);
			}
		}
	}

	protected void removeBackups(Address address) {
		backups.removeAll(address);
	}

	private void distributeLostBackups(Address fallenNodeAddress) {
		distributeBackups(fallenNodeAddress, mySignalsBackup, true);
	}

	private void distributeNewSignalsFromBackups(Address fallenNodeAddress) {
		distributeBackups(fallenNodeAddress, this.backups, false);
	}

	protected void distributeBackups(Address address,
			Multimap<Address, Signal> backups, boolean isBackup) {
		degraded.set(true);
		int membersQty = 1;
		List<Address> members = null;
		if (connection != null) {
			members = connection.getMembers();
			membersQty = members.size();
		}
		BlockingQueue<Signal> newSignals = new LinkedBlockingQueue<Signal>(
				backups.get(address));
		if (membersQty != 1) {
			int sizeToDistribute = newSignals.size() / membersQty;
			Address myAddress = connection.getMyAddress();
			for (int i = 0; i < membersQty; i++) {
				Address futureOwner = members.get(i);
				if (!futureOwner.equals(myAddress)) {
					List<Signal> auxList = new ArrayList<Signal>();
					newSignals.drainTo(auxList, sizeToDistribute);
					if (isBackup) {
						List<Backup> backupList = new ArrayList<Backup>();
						for (Signal s : auxList) {
							backupList.add(new Backup(address, s));
						}
						connection.sendMessageTo(futureOwner,
								new SignalMessage(SignalMessageType.BACK_UPS,
										backupList));
						this.sendBackups.putAll(futureOwner, backupList);
					} else {
						connection
								.sendMessageTo(
										futureOwner,
										new SignalMessage(
												auxList,
												SignalMessageType.BACKUP_REDISTRIBUTION));
						this.sendSignals.putAll(futureOwner, auxList);
					}
				}
			}
			backups.removeAll(address);
		} else {
			if (isBackup) {
				this.backups.putAll(connection.getMyAddress(), newSignals);
			} else {
				this.signals.addAll(newSignals);
				if (connection != null) {
					backups.putAll(connection.getMyAddress(), newSignals);
				}
			}
			backups.removeAll(address);
		}
		degraded.set(false);
	}

	private void distributeBackup(Address owner, Signal signal) {
		this.distributeBackup(owner, signal, null);
	}

	private void distributeBackup(Address signalOwner, Signal signal,
			Backup backup) {
		int membersQty = 1;
		List<Address> users = null;
		if (connection != null) {
			users = connection.getMembers();
			membersQty = users.size();
		}
		if (membersQty != 1) {
			int random = 0;
			while (users.get(random = random(membersQty)).equals(signalOwner))
				;
			Address futureBackupOwner = connection.getMembers().get(random);
			if (connection.getMyAddress().equals(futureBackupOwner)) {
				this.backups.put(signalOwner, signal);
				connection.sendMessageTo(signalOwner, new SignalMessage(
						futureBackupOwner, signal,
						SignalMessageType.ADD_BACKUP_OWNER));
			} else {
				if (backup == null) {
					backup = new Backup(signalOwner, signal);
				}
				connection.sendMessageTo(futureBackupOwner, new SignalMessage(
						// Address not used
						connection.getMyAddress(), backup,
						SignalMessageType.BACK_UP));
				sendBackups.put(futureBackupOwner, backup);
			}
		} else {
			if (connection != null) {
				this.backups.put(connection.getMyAddress(), signal);
			}
		}
	}

	protected void distributeSignals(Address address) {
		distributeSignals(this.signals, address);
	}

	private void distributeSignals(BlockingQueue<Signal> signals,
			Address address) {
		if (signals.isEmpty()) {
			return;
		}
		degraded.set(true);
		int membersQty = connection.getMembersQty();
		synchronized (signals) {
			int sizeToDistribute = signals.size() / membersQty;
			List<Signal> distSignals = new ArrayList<Signal>();
			// Sublist of first sizeToDistribute (if available) signals
			signals.drainTo(distSignals, sizeToDistribute);
			if (address == null) {
				boolean me = true;
				Address addr = null;
				Address myAddress = connection.getMyAddress();
				while (me) {
					int to = random(membersQty);
					addr = connection.getMembers().get(to);
					if (!addr.equals(myAddress)) {
						me = false;
					}
				}
				address = addr;
			}
			connection.sendMessageTo(address, new SignalMessage(distSignals,
					SignalMessageType.YOUR_SIGNALS));
			sendSignals.putAll(address, distSignals);

			// It's the first time, so backup must be distributed
			if (membersQty == 2) {
				for (Signal signal : this.signals) {
					distributeBackup(connection.getMyAddress(), signal);
					this.backups.remove(connection.getMyAddress(), signal);
				}
			}
		}
		degraded.set(false);
	}

	private void distribute(Address address, Object obj) {
		connection.sendMessageTo(address, obj);
	}

	protected void changeBackupOwner(Address newOwner, List<Signal> signals) {
		Multimap<Address, Signal> toRemove;
		ArrayListMultimap<Address, Signal> list = ArrayListMultimap.create();
		toRemove = Multimaps.synchronizedListMultimap(list);
		synchronized (backups) {
		for (Address oldOwner : backups.keySet()) {

				for (Signal signal : backups.get(oldOwner)) {
					// TODO: Ver que pasa si hay dos señales iguales dando
					// vuelta y
					// hay dos backups tambien
					if (signals.contains(signal)) {
						toRemove.put(oldOwner, signal);
						backups.put(newOwner, signal);
					}
				}
			}
		}
		for (Address addr : toRemove.keySet()) {
			for (Signal signal : toRemove.get(addr)) {
				backups.remove(addr, signal);
			}
		}
	}

	private int random(int limit) {
		Random random = new Random();
		return random.nextInt(limit);
	}

	protected void addSignal(Address from, Signal signal) {
		this.signals.add(signal);
		connection.sendMessageTo(from,
				new SignalMessage(connection.getMyAddress(), signal,
						SignalMessageType.ADD_SIGNAL_ACK));
	}

	protected void addSignals(Address from, List<Signal> newSignals, String type) {
		this.signals.addAll(newSignals);
		connection
				.sendMessageTo(
						from,
						new SignalMessage(
								connection.getMyAddress(),
								newSignals,
								type.equals(SignalMessageType.YOUR_SIGNALS) ? SignalMessageType.ADD_SIGNALS_ACK
										: SignalMessageType.BACKUP_REDISTRIBUTION_ACK));
	}

	protected void addBackup(Address from, Backup backup) {
		this.backups.put(backup.getAddress(), backup.getSignal());
		connection.sendMessageTo(from,
				new SignalMessage(connection.getMyAddress(), backup,
						SignalMessageType.ADD_BACKUP_ACK));
	}

	protected void addBackups(Address from, List<Backup> backupList) {
		for (Backup backup : backupList) {
			this.backups.put(backup.getAddress(), backup.getSignal());
		}
		connection.sendMessageTo(from,
				new SignalMessage(connection.getMyAddress(),
						SignalMessageType.ADD_BACKUPS_ACK, backupList));
	}

	protected void findMySimilars(Address from, Signal signal, int id) {
		Result result = findSimilarToAux(signal);
		connection.sendMessageTo(from,
				new SignalMessage(connection.getMyAddress(), result, id,
						SignalMessageType.REQUEST_NOTIFICATION));
	}

	protected void addNotification(SignalMessage notification) {
		notifications.add(notification);
	}

	@Override
	public Result findSimilarTo(Signal signal) throws RemoteException {
		if (signal == null) {
			throw new IllegalArgumentException("Signal cannot be null");
		}
		int requestId = receivedSignals.incrementAndGet();
		if (connection != null) {
			List<Address> addresses = new ArrayList<Address>(
					connection.getMembers());
			addresses.remove(connection.getMyAddress());
			Semaphore semaphore = new Semaphore(addresses.size());
			requests.add(new FindRequest(requestId, addresses,
					addresses.size(), semaphore));
			for (Address address : addresses) {
				connection.sendMessageTo(address, new SignalMessage(signal,
						requestId, SignalMessageType.FIND_SIMILAR));
			}
			semaphore.tryAcquire(addresses.size());
		}
		Result result = findSimilarToAux(signal);
		Iterator<FindRequest> it = requests.iterator();
		while (it.hasNext()) {
			FindRequest request = it.next();
			if (request.getId() == requestId) {
				List<Result> results = request.getResults();
				for (Result otherResult : results) {
					for (Item item : otherResult.items()) {
						result = result.include(item);
					}
				}
				it.remove();
				break;
			}
		}
		return result;
	}

	private Result findSimilarToAux(Signal signal) {
		List<Callable<Result>> tasks = new ArrayList<Callable<Result>>();
		try {
			BlockingQueue<Signal> copy = new LinkedBlockingQueue<Signal>(
					signals);
			for (int i = 0; i < threadsQty; i++) {
				// Generate a copy of the signals so they are not lost
				tasks.add(new Worker(signal, copy));
			}
			List<Future<Result>> futures = executor.invokeAll(tasks);
			Result result = new Result(signal);
			for (Future<Result> future : futures) {
				Result r = future.get();
				for (Item item : r.items()) {
					result = result.include(item);
				}
			}
			return result;
		} catch (InterruptedException e) {
			e.printStackTrace();

		} catch (ExecutionException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}

	private class Worker implements Callable<Result> {

		private BlockingQueue<Signal> workerSignals;
		private Signal workerSignal;

		public Worker(Signal signal, BlockingQueue<Signal> signals) {
			this.workerSignals = signals;
			this.workerSignal = signal;
		}

		@Override
		public Result call() throws Exception {
			Result result = new Result(workerSignal);
			while (true) {
				Signal s = workerSignals.poll();
				if (s == null) {
					return result;
				}
				Result.Item item = new Result.Item(s,
						workerSignal.findDeviation(s));
				result = result.include(item);
			}
		}
	}

	private class NotificationsAnalyzer extends Thread {

		private AtomicBoolean finishedAnalyzer = new AtomicBoolean(false);

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
						distributeBackup(notification.getAddress(),
								notification.getSignal());
						break;
					// case SignalMessageType.ADD_SIGNAL_NACK:
					// sendSignals.remove(notification.getAddress(),
					// notification.getSignal());
					// // Try with someone else
					// distributeNewSignal(notification.getSignal());
					// sendSignals.put(notification.getAddress(),
					// notification.getSignal());
					// break;
					case SignalMessageType.ADD_SIGNALS_ACK:
						sendSignals.remove(notification.getAddress(),
								notification.getSignals());
						connection.broadcastMessage(new SignalMessage(
								notification.getAddress(), notification
										.getSignals(),
								SignalMessageType.CHANGE_BACK_UP_OWNER));
						break;
					case SignalMessageType.BACKUP_REDISTRIBUTION_ACK:
						sendSignals.remove(notification.getAddress(),
								notification.getSignals());
						for (Signal signal : notification.getSignals()) {
							distributeBackup(notification.getAddress(), signal);
						}
						break;
					// case SignalMessageType.ADD_SIGNALS_NACK:
					// sendSignals.remove(notification.getAddress(),
					// notification.getSignals());
					// // Try with someone else
					// distributeSignals(new LinkedBlockingQueue<Signal>(
					// notification.getSignals()), null);
					// sendSignals.putAll(notification.getAddress(),
					// notification.getSignals());
					// break;
					case SignalMessageType.ADD_BACKUP_ACK:
						sendBackups.remove(notification.getAddress(),
								notification.getBackup());
						if (signals.contains(notification.getBackup()
								.getSignal())) {
							mySignalsBackup.put(notification.getAddress(),
									notification.getBackup().getSignal());
						} else {
							connection.sendMessageTo(notification.getBackup()
									.getAddress(), new SignalMessage(
									notification.getAddress(), notification
											.getBackup().getSignal(),
									SignalMessageType.ADD_BACKUP_OWNER));
						}
						break;
					// case SignalMessageType.ADD_BACKUP_NACK:
					// sendBackups.remove(notification.getAddress(),
					// notification.getBackup());
					// // Try with someone else
					// distributeBackup(notification.getAddress(),
					// notification.getSignal(),
					// notification.getBackup());
					// break;
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
														notification
																.getAddress(),
														b.getSignal(),
														SignalMessageType.ADD_BACKUP_OWNER));
							}
						}
						break;

					case SignalMessageType.NEW_NODE:
						distributeSignals(notification.getAddress());
						break;
					case SignalMessageType.BYE_NODE:
						distributeNewSignalsFromBackups(notification
								.getAddress());
						distributeLostBackups(notification.getAddress());
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

	public void changeWhoBackupMySignal(Address address, Signal signal) {
		this.mySignalsBackup.put(address, signal);
	}

}
