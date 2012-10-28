package ar.edu.itba.pod.legajo51048.impl;

import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Collection;
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
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.collections.CollectionUtils;
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

/**
 * Implementation of a node that finds similar signals using multiple threads
 * and distributing the request.
 * 
 * @author Esteban G. Pintos
 * 
 */
public class MultithreadedSignalProcessor implements SPNode, SignalProcessor {

	// It's signals
	private final BlockingQueue<Signal> signals;

	// Signals that have benn distributed and it's waiting for ACK
	private final Multimap<Address, Signal> sendSignals;

	// Back up of ther nodes signals
	private final Multimap<Address, Signal> backups;

	// Map containing who is the owner of the backup of "signals"
	private final Multimap<Address, Signal> mySignalsBackup;

	// Backups that have been distributed and it's waiting for ACK
	private final Multimap<Address, Backup> sendBackups;

	// Queue of notifications to analyze
	private final BlockingQueue<SignalMessage> notifications;

	// List containing the find similar request send to other nodes
	private final List<FindRequest> requests;

	// Processing threads
	private final ExecutorService executor;
	private NotificationsAnalyzer notificationsAnalyzer;
	private final int threadsQty;

	// Quantity of received find similar requests.
	private AtomicInteger receivedSignals = new AtomicInteger(0);

	// Connection implementation to a cluster
	private Connection connection = null;

	public MultithreadedSignalProcessor(int threadsQty) {
		ArrayListMultimap<Address, Signal> list = ArrayListMultimap.create();
		ArrayListMultimap<Address, Backup> list2 = ArrayListMultimap.create();
		ArrayListMultimap<Address, Signal> list4 = ArrayListMultimap.create();
		ArrayListMultimap<Address, Signal> list3 = ArrayListMultimap.create();
		this.backups = Multimaps.synchronizedListMultimap(list);
		this.sendBackups = Multimaps.synchronizedListMultimap(list2);
		this.mySignalsBackup = Multimaps.synchronizedListMultimap(list4);
		this.notifications = new LinkedBlockingQueue<SignalMessage>();
		this.requests = new ArrayList<FindRequest>();
		this.signals = new LinkedBlockingQueue<Signal>();
		this.sendSignals = Multimaps.synchronizedListMultimap(list3);
		this.executor = Executors.newFixedThreadPool(threadsQty);
		this.threadsQty = threadsQty;
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
		this.startNotificationsAnalyzer();

	}

	/**
	 * Create and start notifications analyzer thread.
	 */
	private void startNotificationsAnalyzer() {
		this.notificationsAnalyzer = new NotificationsAnalyzer(signals,
				notifications, sendSignals, this, mySignalsBackup, sendBackups,
				requests, connection);
		this.notificationsAnalyzer.start();
	}

	@Override
	public void exit() throws RemoteException {
		signals.clear();
		backups.clear();
		notifications.clear();
		receivedSignals = new AtomicInteger(0);
		if (connection != null) {
			connection.broadcastMessage(SignalMessageType.BYE_NODE);
			connection.disconnect();
		}
		notificationsAnalyzer.finish();
		notificationsAnalyzer.interrupt();
		try {
			notificationsAnalyzer.join(1000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
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

	/*
	 * Distributes a signal to a random node. If it's assigned to this node,
	 * then the backup it's distributed. If there is only 1 node (this one), the
	 * signal is added to this node and the backup also.
	 */
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
			// TODO: Ver que hacer con los backups si no hay conexi??n
			if (connection != null) {
				this.backups.put(connection.getMyAddress(), signal);
				this.mySignalsBackup.put(connection.getMyAddress(), signal);
			}
		}
	}

	/**
	 * Removes all the backups that this node has of the node with "address"
	 * 
	 * @param address
	 */
	protected void removeBackups(Address address) {
		backups.removeAll(address);
	}

	/**
	 * When a node falls, the backups that it had are redistributed to the other
	 * nodes.
	 * 
	 * @param fallenNodeAddress
	 */
	protected void distributeLostBackups(Address fallenNodeAddress) {
		distributeBackups(fallenNodeAddress, mySignalsBackup, true);
	}

	/**
	 * When a node falls, the signals that it had are redistributed to the other
	 * nodes as new signals.
	 * 
	 * @param fallenNodeAddress
	 */
	protected void distributeNewSignalsFromBackups(Address fallenNodeAddress) {
		distributeBackups(fallenNodeAddress, this.backups, false);
	}

	/**
	 * Distributes signals/backups according to the quantity of nodes
	 * (randomly).
	 * 
	 * @param address
	 * @param backups
	 * @param isBackup
	 *            Is backup or signal
	 */
	private void distributeBackups(Address address,
			Multimap<Address, Signal> backups, boolean isBackup) {
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
				// TODO: Y si soy yo?
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
												SignalMessageType.SIGNAL_REDISTRIBUTION));
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
	}

	/**
	 * Distributes randomly a backup from owner.
	 * 
	 * @param owner
	 *            Owner of the signal
	 * @param signal
	 *            Signal
	 */
	protected void distributeBackup(Address owner, Signal signal) {
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
			Address futureBackupOwner = users.get(random);
			if (connection.getMyAddress().equals(futureBackupOwner)) {
				this.backups.put(signalOwner, signal);
				connection.sendMessageTo(signalOwner, new SignalMessage(
						connection.getMyAddress(), signal,
						SignalMessageType.CHANGE_WHO_BACK_UP_MYSIGNAL));
			} else {
				if (backup == null) {
					backup = new Backup(signalOwner, signal);
				}
				connection.sendMessageTo(futureBackupOwner, new SignalMessage(
						null, backup, SignalMessageType.BACK_UP));
				sendBackups.put(futureBackupOwner, backup);
			}
		} else {
			if (connection != null) {
				this.backups.put(connection.getMyAddress(), signal);
				// TODO: is this neccesary?
				if (this.signals.contains(signal)) {
					this.mySignalsBackup.put(connection.getMyAddress(), signal);
				}
			}
		}
	}

	/**
	 * Distribute signals to the node with "address"
	 * 
	 * @param address
	 *            Node address
	 */
	protected void distributeSignals(Address address) {
		distributeSignals(this.signals, address);
	}

	private void distributeSignals(BlockingQueue<Signal> signalsQueue,
			Address to) {
		synchronized (signalsQueue) {
			if (signalsQueue.isEmpty()) {
				return;
			}
			int membersQty = 0;
			if (connection != null) {
				membersQty = connection.getMembersQty();
			}
			int sizeToDistribute = signalsQueue.size() / membersQty;
			List<Signal> distSignals = new ArrayList<Signal>();
			System.out.println("signals original size:" + signalsQueue.size());
			// Sublist of first sizeToDistribute (if available) signals
			signalsQueue.drainTo(distSignals, sizeToDistribute);
			connection.sendMessageTo(to, new SignalMessage(distSignals,
					SignalMessageType.YOUR_SIGNALS));
			sendSignals.putAll(to, distSignals);
			System.out.println("distSignals size: " + distSignals.size());

			// There was only 1 node, so backup must be distributed
			if (membersQty == 2) {
				System.out.println("signals size: " + signalsQueue.size());
				for (Signal signal : signalsQueue) {
					// Distribute backup of those signals that are still owned
					// by this node
					distributeBackup(connection.getMyAddress(), signal);
					this.backups.remove(connection.getMyAddress(), signal);
				}
			}
		}
	}

	/**
	 * * Goes throw this.backups changing the owner of the backup containing
	 * signals to newOwner
	 * 
	 * @param newOwner
	 *            New owner of the backup
	 * @param signals
	 * @param backupMap
	 */
	protected void changeBackupOwner(Address newOwner, List<Signal> signals) {
		System.out.println("changeBackupOwner");
		changeBackupOwner(newOwner, signals, this.backups, true);
	}

	/**
	 * * Goes throw this.mySignalsBackup changing the owner of the backup
	 * containing signals to newOwner
	 * 
	 * @param address
	 *            New owner of the backup
	 * @param signal
	 */
	protected void changeWhoBackupMySignal(Address address, Signal signal) {
		System.out.println("changeWhoBackupMySignal");
		List<Signal> list = new ArrayList<Signal>();
		list.add(signal);
		System.out.println("tamaño list:"+list.size());
		changeBackupOwner(address, list, this.mySignalsBackup, false);
	}

	/**
	 * * Goes throw backupMap changing the owner of the backup containing
	 * signals to newOwner
	 * 
	 * @param newOwner
	 *            New owner of the backup
	 * @param signals
	 * @param backupMap
	 * @param changeOwner
	 *            Indicates if the owner of the signals has to be notified of
	 *            the changed
	 */
	@SuppressWarnings("unchecked")
	protected void changeBackupOwner(Address newOwner, List<Signal> signals,
			Multimap<Address, Signal> backupMap, boolean changeOwner) {
		Multimap<Address, Signal> toRemove;
		ArrayListMultimap<Address, Signal> list = ArrayListMultimap.create();
		toRemove = Multimaps.synchronizedListMultimap(list);
		List<Signal> indexed = new ArrayList<Signal>();
		synchronized (backupMap) {
			for (Address oldOwner : backupMap.keySet()) {
				for (Signal signal : backupMap.get(oldOwner)) {
					// TODO: Ver que pasa si hay dos señales iguales dando
					// vuelta y
					// hay dos backups tambien
					if (signals.contains(signal)) {
						if(changeOwner){
							indexed.add(signal);
						}
						toRemove.put(oldOwner, signal);
						backupMap.put(newOwner, signal);
					}
				}
			}
		}
		for (Address addr : toRemove.keySet()) {
			for (Signal signal : toRemove.get(addr)) {
				if (changeOwner) {
					//Tell the owner of the signals who has his backups
					connection.sendMessageTo(newOwner, new SignalMessage(
							connection.getMyAddress(), signal,
							SignalMessageType.CHANGE_WHO_BACK_UP_MYSIGNAL));
				}
				backupMap.remove(addr, signal);
			}
		}
		
		if(!changeOwner){
			//Those that didn't appeared in the map
			Collection<Signal> disjunction = CollectionUtils.disjunction(
					signals, indexed);
			for (Signal signal : disjunction) {
				backupMap.put(newOwner,signal);
				System.out.println("se agrega nueva");
			}
		}
	}

	/**
	 * Calculates random value between 0 and limit.
	 * 
	 * @param limit
	 * @return Random number
	 */
	private int random(int limit) {
		Random random = new Random();
		return random.nextInt(limit);
	}

	/**
	 * 
	 * Adds a signal to this node and returns ADD_SIGNAL_ACK to "from"
	 * 
	 * @param from
	 *            Node that send me the order to add the signal
	 * @param signal
	 *            Signal to add
	 */
	protected void addSignal(Address from, Signal signal) {
		this.signals.add(signal);
		connection.sendMessageTo(from,
				new SignalMessage(connection.getMyAddress(), signal,
						SignalMessageType.ADD_SIGNAL_ACK));
	}

	/**
	 * Adds a list of signals to this node and returns ADD_SIGNALS_ACK if the
	 * order whas to add brand new signals, and BACKUP_REDISTRIBUTION_ACK if the
	 * order whas to add the signals of a fallen node
	 * 
	 * @param from
	 * @param newSignals
	 * @param type
	 *            YOUR_SIGNALS or BACKUP_REDISTRIBUTION_ACK
	 */
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

	/**
	 * Adds a backup to this node and returns ADD_BACKUP_ACK
	 * 
	 * @param from
	 *            Node that send me the order to add the backup
	 * @param backup
	 */
	protected void addBackup(Address from, Backup backup) {
		this.backups.put(backup.getAddress(), backup.getSignal());
		connection.sendMessageTo(from,
				new SignalMessage(connection.getMyAddress(), backup,
						SignalMessageType.ADD_BACKUP_ACK));
	}

	/**
	 * Adds a list of backups to this node and returns ADD_BACKUPS_ACK
	 * 
	 * @param from
	 *            Node that send me the order to add the backups
	 * @param backupList
	 */
	protected void addBackups(Address from, List<Backup> backupList) {
		for (Backup backup : backupList) {
			this.backups.put(backup.getAddress(), backup.getSignal());
		}
		connection.sendMessageTo(from,
				new SignalMessage(connection.getMyAddress(),
						SignalMessageType.ADD_BACKUPS_ACK, backupList));
	}

	/**
	 * Adds a notification to the queue.
	 * 
	 * @param notification
	 *            New notification
	 */
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

	/**
	 * Find similars to signal and returns the Result to from.
	 * 
	 * @param from
	 *            Node that send the find similar request
	 * @param signal
	 *            Signal to analyze
	 * @param id
	 *            Id of the request
	 */
	protected void findMySimilars(Address from, Signal signal, int id) {
		Result result = findSimilarToAux(signal);
		connection.sendMessageTo(from,
				new SignalMessage(connection.getMyAddress(), result, id,
						SignalMessageType.REQUEST_NOTIFICATION));
	}

	/**
	 * Finds similar signals of signal
	 * 
	 * @param signal
	 * @return Result of similar signals
	 */
	private Result findSimilarToAux(Signal signal) {
		List<Callable<Result>> tasks = new ArrayList<Callable<Result>>();
		try {
			BlockingQueue<Signal> copy = new LinkedBlockingQueue<Signal>(
					signals);
			for (int i = 0; i < threadsQty; i++) {
				// Generate a copy of the signals so they are not lost
				tasks.add(new FindSimilarWorker(signal, copy));
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
			// TODO Auto-generated catch block
			e.printStackTrace();

		} catch (ExecutionException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}
}