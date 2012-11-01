package ar.edu.itba.pod.legajo51048.impl;

import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
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

/**
 * Implementation of a node that finds similar signals using multiple threads
 * and distributing the request.
 * 
 * @author Esteban G. Pintos
 * 
 */
public class MultithreadedSignalProcessor implements SPNode, SignalProcessor {

	// Node's signals
	private final BlockingQueue<Signal> signals;

	// Signals that have been distributed and still waiting for ACK
	private final Multimap<Address, Signal> sendSignals;

	// Back ups of ther node's signals
	private final Multimap<Address, Signal> backups;

	// Map containing who is the owner of the back ups of this node
	private final Multimap<Address, Signal> mySignalsBackup;

	// Backups that have been distributed and still waiting for ACK
	private final Multimap<Address, Backup> sendBackups;

	// Messages that have been send too change backups owners.
	private final Multimap<Address, Signal> sendChangeWhoBackup;

	// Tasks to be done when a node falls
	private final ConcurrentMap<Address, Semaphore> tasksDone;

	// Queue of notifications to analyze
	private final BlockingQueue<SignalMessage> notifications;

	// List containing the find similar requests send to other nodes
	private final ConcurrentMap<Integer, FindRequest> requests;

	// Processing threads
	private ExecutorService executor;
	private NotificationsAnalyzer notificationsAnalyzer;
	private final int threadsQty;

	// Quantity of received find similar requests.
	private AtomicInteger receivedFind = new AtomicInteger(0);
	
	private AtomicInteger receivedSignals = new AtomicInteger(0);

	// Connection implementation to a cluster
	private Connection connection = null;

	private static final int CHUNK_SIZE = 300;

	public MultithreadedSignalProcessor(int threadsQty) {
		ArrayListMultimap<Address, Signal> list = ArrayListMultimap.create();
		ArrayListMultimap<Address, Backup> list2 = ArrayListMultimap.create();
		ArrayListMultimap<Address, Signal> list3 = ArrayListMultimap.create();
		ArrayListMultimap<Address, Signal> list4 = ArrayListMultimap.create();
		ArrayListMultimap<Address, Signal> list5 = ArrayListMultimap.create();
		this.backups = Multimaps.synchronizedListMultimap(list);
		this.sendBackups = Multimaps.synchronizedListMultimap(list2);
		this.sendSignals = Multimaps.synchronizedListMultimap(list3);
		this.mySignalsBackup = Multimaps.synchronizedListMultimap(list4);
		this.sendChangeWhoBackup = Multimaps.synchronizedListMultimap(list5);
		this.tasksDone = new ConcurrentHashMap<Address, Semaphore>();
		this.notifications = new LinkedBlockingQueue<SignalMessage>();
		this.requests = new ConcurrentHashMap<Integer, FindRequest>();
		this.signals = new LinkedBlockingQueue<Signal>();
		this.executor = Executors.newFixedThreadPool(threadsQty);
		this.threadsQty = threadsQty;
	}

	/******************** SPNODE IMPLEMENTATION ********************/
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

	@Override
	public void exit() throws RemoteException {
		signals.clear();
		backups.clear();
		notifications.clear();
		mySignalsBackup.clear();
		sendBackups.clear();
		sendSignals.clear();
		sendChangeWhoBackup.clear();
		requests.clear();
		receivedFind = new AtomicInteger(0);
		if (connection != null) {
			connection.disconnect();
			connection = null;
		}
		try {
			if (notificationsAnalyzer != null) {
				notificationsAnalyzer.finish();
				notificationsAnalyzer.interrupt();
				notificationsAnalyzer.join();
			}
			executor.shutdown();
			while (!executor.awaitTermination(1000, TimeUnit.MILLISECONDS))
				;
			this.executor = Executors.newFixedThreadPool(threadsQty);
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public NodeStats getStats() throws RemoteException {
		// int qty = 0;
		// synchronized (mySignalsBackup) {
		//
		// for (Address addr : mySignalsBackup.keySet()) {
		// System.out.println(addr + " "
		// + mySignalsBackup.get(addr).size());
		// for (Signal s : mySignalsBackup.get(addr)) {
		// qty++;
		// }
		// }
		// System.out.println("mySignalsbackupsize: " + qty);
		// }
		return new NodeStats("cluster " + connection.getClusterName(),
				receivedSignals.longValue(), signals.size(), backups.size(),
				true);
	}
	
	/**
	 * Create and start notifications analyzer thread.
	 */
	private void startNotificationsAnalyzer() {
		this.notificationsAnalyzer = new NotificationsAnalyzer(signals,
				notifications, sendSignals, this, mySignalsBackup, sendBackups,
				requests, sendChangeWhoBackup, tasksDone, connection);
		this.notificationsAnalyzer.start();
	}

	/******************** SIGNALPROCESSOR IMPLEMENTATION ********************/

	@Override
	public void add(Signal signal) throws RemoteException {
		distributeNewSignal(signal);
	}

	@Override
	public Result findSimilarTo(Signal signal) throws RemoteException {
		if (signal == null) {
			throw new IllegalArgumentException("Signal cannot be null");
		}
		int requestId = receivedFind.incrementAndGet();
		Result result = null;
		List<Address> addresses = null;
		if (connection != null) {
			addresses = new ArrayList<Address>(connection.getMembers());
		}
		if (addresses != null && addresses.size() > 1) {
			// I don't have to send the request to me

			Semaphore semaphore = new Semaphore(0);
			FindRequest request = new FindRequest(requestId, signal, addresses,
					semaphore);
			this.requests.put(requestId, request);
			connection.broadcastMessage(new SignalMessage(signal, requestId,
					SignalMessageType.FIND_SIMILAR));

			// This while will wait for Results and will not finish until ALL
			// the results are found (includes 1-tolerance)
			try {
				while (!semaphore.tryAcquire(request.getQty(), 10000,
						TimeUnit.MILLISECONDS)) {
					System.out.println("Esperando resultados: "
							+ semaphore.availablePermits());
				}
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			System.out.println("llegaron los resultados memberQty: "
					+ request.getQty());
			result = findSimilarToAux(signal);
			if (request != null) {
				List<Result> results = request.getResults();
				for (Result otherResult : results) {
					for (Item item : otherResult.items()) {
						result = result.include(item);
					}
				}
			}
		} else {
			result = findSimilarToAux(signal);
		}
		return result;
	}

	/******************** FIND SIMILAR AUXILIARY METHODS ********************/

	/**
	 * Find similars to signal and returns the Result to the node that send the
	 * order.
	 * 
	 * @param from
	 *            Node that send the find similar request
	 * @param signal
	 *            Signal to analyze
	 * @param id
	 *            Id of the request
	 * @return Result of similar signals
	 */
	protected void findMySimilars(Address from, Signal signal, int id) {
		System.out.println(connection.getMyAddress() + " findingSimilars...");
		Result result = findSimilarToAux(signal);
		connection.sendMessageTo(from,
				new SignalMessage(connection.getMyAddress(), result, id,
						SignalMessageType.REQUEST_NOTIFICATION));
		System.out.println(connection.getMyAddress()
				+ "finishedFindingSimilars....");
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
					this.signals);
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
			e.printStackTrace();

		} catch (ExecutionException e) {
			e.printStackTrace();
		}
		return null;
	}

	/******************** DISTRIBUTION METHODS ********************/

	/**
	 * Distributes a signal to a random node. If it's assigned to this node,
	 * then the backup it's distributed. If there is only 1 node (this one), the
	 * signal is added to this node and the backup also.
	 * 
	 * @param Signal
	 *            signal added
	 **/
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
				distributeBackup(myAddress, null, signal);
			} else {
				this.sendSignals.put(futureOwner, signal);
				connection.sendMessageTo(futureOwner, new SignalMessage(
						connection.getMyAddress(), signal,
						SignalMessageType.YOUR_SIGNAL));
			}
		} else {
			this.signals.add(signal);
			if (connection != null) {
				this.backups.put(connection.getMyAddress(), signal);
				this.mySignalsBackup.put(connection.getMyAddress(), signal);
			}
		}
	}

	/**
	 * When a node falls, if the fallen node had backups, they are lost, so each
	 * node creates new ones using the "mySignalsBackup" map.
	 * 
	 * @param fallenNodeAddress
	 *            Address of the fallen node
	 */
	protected int distributeLostBackups(Address fallenNodeAddress) {
		int membersQty = 1;
		List<Address> members = null;
		Semaphore sem = new Semaphore(0);
		tasksDone.put(fallenNodeAddress, sem);
		if (connection != null) {
			members = connection.getMembers();
			membersQty = members.size();
		}
		int ret = 0;
		BlockingQueue<Signal> newSignals = new LinkedBlockingQueue<Signal>(
				this.mySignalsBackup.get(fallenNodeAddress));
		if (newSignals.isEmpty()) {
			return ret;
		}
		if (membersQty != 1) {
			int mod = 0;
			int sizeToDistribute = 0;
			mod = newSignals.size() % (membersQty - 1);
			sizeToDistribute = mod == 0 ? newSignals.size() / (membersQty - 1)
					: (newSignals.size() + mod) / (membersQty - 1);
			Address myAddress = connection.getMyAddress();
			int random = 0;
			List<Address> chosen = new ArrayList<Address>();
			// Can't send the backups to me, because the signals are mine
			chosen.add(myAddress);
			int cicles = membersQty - 1;
			for (int i = 0; i < cicles; i++) {
				while (chosen
						.contains(members.get(random = random(membersQty))))
					;
				Address futureOwner = members.get(random);
				chosen.add(futureOwner);

				if (i == cicles - 1) {
					sizeToDistribute = newSignals.size();
				}
				if (!futureOwner.equals(myAddress)) {
					int chunks = sizeToDistribute / CHUNK_SIZE + 1;
					for (int j = 0; j < chunks; j++) {
						List<Signal> auxList = new ArrayList<Signal>();
						newSignals.drainTo(auxList, CHUNK_SIZE);

						List<Backup> backupList = new ArrayList<Backup>();
						// Distribute those backups that were mine and got lost
						// with
						// the fallen node
						for (Signal s : auxList) {
							backupList.add(new Backup(myAddress, s));
						}
						if (!backupList.isEmpty()) {
							this.sendBackups.putAll(futureOwner, backupList);
							connection.sendMessageTo(futureOwner,
									new SignalMessage(
											connection.getMyAddress(),
											fallenNodeAddress,
											SignalMessageType.BACK_UPS,
											backupList));
							ret += backupList.size();
						}
					}
				} else {
					System.out.println("no deberia pasar");
				}
			}
			this.mySignalsBackup.removeAll(fallenNodeAddress);
			return ret;
		}
		return ret;
	}

	/**
	 * When a node falls, the signals that had are lost, so each node
	 * distributes new signals using the "backups" map.
	 * 
	 * @param fallenNodeAddress
	 *            Address of the fallen node
	 */
	protected int distributeNewSignalsFromBackups(Address fallenNodeAddress) {
		int membersQty = 1;
		List<Address> members = null;
		Semaphore sem = new Semaphore(0);
		tasksDone.put(fallenNodeAddress, sem);
		if (connection != null) {
			members = connection.getMembers();
			membersQty = members.size();
		}
		BlockingQueue<Signal> newSignals = null;
		synchronized (backups) {
			newSignals = new LinkedBlockingQueue<Signal>(
					backups.get(fallenNodeAddress));
		}
		if (newSignals.isEmpty()) {
			return 0;
		}
		int ret = 0;
		if (membersQty != 1) {
			int mod = 0;
			int sizeToDistribute = 0;
			mod = newSignals.size() % membersQty;
			sizeToDistribute = mod == 0 ? newSignals.size() / membersQty
					: (newSignals.size() + mod) / membersQty;
			Address myAddress = connection.getMyAddress();
			int random = 0;
			List<Address> chosen = new ArrayList<Address>();
			int cicles = membersQty;
			for (int i = 0; i < cicles; i++) {
				while (chosen
						.contains(members.get(random = random(membersQty))))
					;
				Address futureOwner = members.get(random);
				chosen.add(futureOwner);
				if (i == cicles - 1) {
					sizeToDistribute = newSignals.size();
				}
				List<Signal> auxList = new ArrayList<Signal>();
				if (!futureOwner.equals(myAddress)) {
					int chunks = sizeToDistribute / CHUNK_SIZE + 1;
					auxList = new ArrayList<Signal>();
					newSignals.drainTo(auxList, CHUNK_SIZE);
					for (int j = 0; j < chunks; j++) {
						if (!auxList.isEmpty()) {
							this.sendSignals.putAll(futureOwner, auxList);
							connection
									.sendMessageTo(
											futureOwner,
											new SignalMessage(
													connection.getMyAddress(),
													fallenNodeAddress,
													auxList,
													SignalMessageType.GENERATE_NEW_SIGNALS_FROM_BACKUP));

						}
					}
				} else {
					auxList = new ArrayList<Signal>();
					newSignals.drainTo(auxList, sizeToDistribute);

					this.signals.addAll(auxList);
					for (Signal signal : auxList) {
						distributeBackup(connection.getMyAddress(),
								fallenNodeAddress, signal);
					}

				}

				ret += auxList.size();
			}
			backups.removeAll(fallenNodeAddress);
			return ret;

		} else {
			this.signals.addAll(newSignals);
			if (connection != null) {
				this.backups.removeAll(fallenNodeAddress);
				this.backups.putAll(connection.getMyAddress(), newSignals);
			}
			backups.putAll(connection.getMyAddress(),
					this.mySignalsBackup.get(fallenNodeAddress));
			this.mySignalsBackup.clear();
			this.mySignalsBackup
					.putAll(connection.getMyAddress(), this.signals);
			return ret;
		}
	}

	/**
	 * Distributes randomly a backup.
	 * 
	 * @param owner
	 *            Owner of the signal
	 * 
	 * @param fallenNodeAddress
	 *            If this method is call during fallen node distribution, this
	 *            address is the one of the fallen node, else must be null.
	 * @param signal
	 *            Signal
	 * 
	 */
	protected void distributeBackup(Address signalOwner,
			Address fallenNodeAddress, Signal signal) {
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
				if (!sendChangeWhoBackup.put(signalOwner, signal)) {
					System.out.println("no deberia pasar, put al sendChange");
				}
				connection.sendMessageTo(signalOwner, new SignalMessage(
						connection.getMyAddress(), fallenNodeAddress, signal,
						SignalMessageType.CHANGE_WHO_BACK_UP_MYSIGNAL));
			} else {
				Backup backup = new Backup(signalOwner, signal);
				sendBackups.put(futureBackupOwner, backup);
				connection.sendMessageTo(futureBackupOwner, new SignalMessage(
						connection.getMyAddress(), fallenNodeAddress, backup,
						SignalMessageType.BACK_UP));
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
	 * Distribute signals to a node. The quantity of nodes distributed is
	 * calculated according to the quanitity of members in the cluster
	 * 
	 * @param address
	 *            Destination node
	 */
	protected void distributeSignals(Address to) {
		int sizeToDistribute = 0;
		int membersQty = 0;
		synchronized (this.signals) {

			if (this.signals.isEmpty()) {
				return;
			}
			if (connection != null) {
				membersQty = connection.getMembersQty();
			}
			sizeToDistribute = this.signals.size() / membersQty;
		}
		BlockingQueue<Signal> copy = new LinkedBlockingQueue<Signal>();
		if (this.signals.drainTo(copy, sizeToDistribute) == 0) {
			System.out.println("drainTo no deberia pasar");
		}
		System.out.println("a distribuir " + copy.size());
		int chunks = copy.size() / CHUNK_SIZE + 1;
		List<Signal> distributedList = new ArrayList<Signal>();
		for (int i = 0; i < chunks; i++) {
			List<Signal> distSignals = new ArrayList<Signal>();
			// Sublist of first sizeToDistribute (if available) signals
			if (copy.drainTo(distSignals, CHUNK_SIZE) == 0) {
				break;
			}
			// If theres nothing to send
			if (!distSignals.isEmpty()) {
				if (!this.sendSignals.putAll(to, distSignals)) {
					System.out
							.println("no deberia pasar agregando a sendSignals");
				}
				connection.sendMessageTo(to,
						new SignalMessage(connection.getMyAddress(),
								distSignals, SignalMessageType.YOUR_SIGNALS));
				distributedList.addAll(distSignals);
			}
		}
		System.out.println("distribuidas " + distributedList.size());
		// There was only 1 node, so backup must be distributed
		if (membersQty == 2) {
			// Removes all, because then will be added again.
			this.mySignalsBackup.removeAll(connection.getMyAddress());
			for (Signal signal : this.signals) {
				// Distribute backup of those signals that are still owned
				// by this node
				distributeBackup(connection.getMyAddress(), null, signal);
			}
			this.backups.get(connection.getMyAddress()).removeAll(this.signals);
		} else {
			removeWhoBackupMySignal(null, distributedList);
		}

	}

	/******************** CHANGE BACKUPS METHODS ********************/

	/**
	 * * If this node had a back up of signals, it will be changed by the new
	 * owner.
	 * 
	 * @param newOwner
	 *            New owner of the backup
	 * @param oldOwner
	 *            Old owner of the signals
	 * @param signals
	 *            Signals which are know backed up by a new owner.
	 */
	protected void changeBackupOwner(Address oldOwner, Address newOwner,
			List<Signal> signals) {
		if (this.backups.get(oldOwner).removeAll(signals)) {
			if (!this.backups.putAll(newOwner, signals)) {
				System.out.println("no deberia pasar putAll");
			}
			for (Signal signal : signals) {
				if (!sendChangeWhoBackup.put(newOwner, signal)) {
					System.out.println("no deberia pasar, put al sendChange 2");
				}
				connection.sendMessageTo(newOwner,
						new SignalMessage(connection.getMyAddress(), signal,
								SignalMessageType.CHANGE_WHO_BACK_UP_MYSIGNAL));
			}
		}
	}

	/**
	 * My signals where distributed to another node, so I must removed them from
	 * mySignalsBackup
	 * 
	 * @param newOwner
	 *            The owner of the backup
	 * @param signals
	 *            Signals distributed
	 */
	protected void removeWhoBackupMySignal(Address newOwner,
			List<Signal> signals) {
		for (Address addr : connection.getMembers()) {
			this.mySignalsBackup.get(addr).removeAll(signals);
		}
	}

	/**
	 * * A signal of this node is backed up by a new node, so this change has to
	 * be made in mySignalsBackup
	 * 
	 * @param address
	 *            New owner of the backup
	 * @param signal
	 *            Signal backed up
	 */
	protected void changeWhoBackupMySignal(Address from, Address address,
			Address fallenNodeAddress, Signal signal, boolean remove) {

		if (remove) {
			this.mySignalsBackup.get(connection.getMyAddress()).remove(signal);
		}
		this.mySignalsBackup.put(address, signal);
		if (from != null) {
			connection.sendMessageTo(from,
					new SignalMessage(connection.getMyAddress(),
							fallenNodeAddress, signal,
							SignalMessageType.CHANGE_WHO_BACK_UP_MYSIGNAL_ACK));
		}

	}

	/******************** ADD SIGNAL/S METHODS ********************/

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
		receivedSignals.incrementAndGet();
		connection.sendMessageTo(from,
				new SignalMessage(connection.getMyAddress(), signal,
						SignalMessageType.ADD_SIGNAL_ACK));
	}

	/**
	 * Adds a list of signals to this node and returns ADD_SIGNALS_ACK if the
	 * order whas to add brand new signals, and
	 * GENERATE_NEW_SIGNALS_FROM_BACKUP_ACK if the order whas to add the signals
	 * of a fallen node
	 * 
	 * @param from
	 *            Node that send me the order
	 * @param newSignals
	 *            New signals to add
	 * @param type
	 *            YOUR_SIGNALS or GENERATE_NEW_SIGNALS_FROM_BACKUP_ACK
	 */
	protected void addSignals(Address from, Address fallenNodeAddress,
			List<Signal> newSignals, String type) {
		this.signals.addAll(newSignals);
		receivedSignals.addAndGet(newSignals.size());
		connection
				.sendMessageTo(
						from,
						new SignalMessage(
								connection.getMyAddress(),
								fallenNodeAddress,
								newSignals,
								type.equals(SignalMessageType.YOUR_SIGNALS) ? SignalMessageType.ADD_SIGNALS_ACK
										: SignalMessageType.GENERATE_NEW_SIGNALS_FROM_BACKUP_ACK));
	}

	/******************** ADD BACKUP/S METHODS ********************/

	/**
	 * Adds a backup to this node and returns ADD_BACKUP_ACK
	 * 
	 * @param from
	 *            Node that send me the order to add the backup
	 * @param backup
	 *            New backup
	 */
	protected void addBackup(Address from, Address fallenNodeAddress,
			Backup backup) {
		this.backups.put(backup.getAddress(), backup.getSignal());
		connection.sendMessageTo(from,
				new SignalMessage(connection.getMyAddress(), fallenNodeAddress,
						backup, SignalMessageType.ADD_BACKUP_ACK));
	}

	/**
	 * Adds a list of backups to this node and returns ADD_BACKUPS_ACK
	 * 
	 * @param from
	 *            Node that send me the order to add the backups
	 * @param backupList
	 *            New backup list
	 */
	protected void addBackups(Address from, Address fallenNodeAddress,
			List<Backup> backupList) {
		for (Backup backup : backupList) {
			this.backups.put(backup.getAddress(), backup.getSignal());
		}
		connection.sendMessageTo(from,
				new SignalMessage(connection.getMyAddress(), fallenNodeAddress,
						SignalMessageType.ADD_BACKUPS_ACK, backupList));
	}

	/**
	 * Adds a new notification.
	 * 
	 * @param notification
	 *            New notification
	 */
	protected void addNotification(SignalMessage notification) {
		notifications.add(notification);
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

}
