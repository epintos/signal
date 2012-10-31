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

	// Messages that have been send two change backups owners.
	private final Multimap<Address, Signal> sendChangeWhoBackup;

	private final ConcurrentMap<Address, Semaphore> tasksDone;

	// Queue of notifications to analyze
	private final BlockingQueue<SignalMessage> notifications;

	// List containing the find similar request send to other nodes
	private final ConcurrentMap<Integer, FindRequest> requests;

	// Processing threads
	private ExecutorService executor;
	private NotificationsAnalyzer notificationsAnalyzer;
	private final int threadsQty;

	// Quantity of received find similar requests.
	private AtomicInteger receivedSignals = new AtomicInteger(0);

	private AtomicBoolean findingSimilars = new AtomicBoolean(false);

	// Connection implementation to a cluster
	private Connection connection = null;

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
				requests, sendChangeWhoBackup, tasksDone, connection);
		this.notificationsAnalyzer.start();
	}

	@Override
	public void exit() throws RemoteException {
		signals.clear();
		backups.clear();
		notifications.clear();
		mySignalsBackup.clear();
		sendBackups.clear();
		sendSignals.clear();
		requests.clear();
		receivedSignals = new AtomicInteger(0);
		if (connection != null) {
			connection.broadcastMessage(new SignalMessage(connection
					.getMyAddress(), SignalMessageType.BYE_NODE));
			if (connection.getMembersQty() == 1) {
				connection.close();
				connection.disconnect();
			} else {
				connection.close();
			}
		}
		try {
			if (notificationsAnalyzer != null) {
				notificationsAnalyzer.finish();
				notificationsAnalyzer.interrupt();
				notificationsAnalyzer.join(1000);
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
		int qty = 0;
		synchronized (mySignalsBackup) {

			for (Address addr : mySignalsBackup.keySet()) {
				System.out.println(addr + " "
						+ mySignalsBackup.get(addr).size());
				for (Signal s : mySignalsBackup.get(addr)) {
					qty++;
				}
			}
			System.out.println("mySignalsbackupsize: " + qty);
		}
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
				distributeBackup(myAddress, null, signal);
			} else {
				this.sendSignals.put(futureOwner, signal);
				connection.sendMessageTo(futureOwner, new SignalMessage(
						connection.getMyAddress(), signal,
						SignalMessageType.YOUR_SIGNAL));
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
	 * When a node falls, the backups that it had are redistributed to the other
	 * nodes.
	 * 
	 * @param fallenNodeAddress
	 */
	protected int distributeLostBackups(Address fallenNodeAddress) {
		int membersQty = 1;
		List<Address> members = null;
		Semaphore sem = new Semaphore(0);
		tasksDone.put(fallenNodeAddress, sem);
		System.out.println("semaforo permits: "
				+ tasksDone.get(fallenNodeAddress).availablePermits());
		if (connection != null) {
			members = connection.getMembers();
			membersQty = members.size();
		}
		int ret = 0;
		BlockingQueue<Signal> newSignals = new LinkedBlockingQueue<Signal>(
				this.mySignalsBackup.get(fallenNodeAddress));
		// System.out.println("tamaño posta: " + newSignals.size());
		if (newSignals.isEmpty()) {
			return ret;
		}
		if (membersQty != 1) {
			int mod = 0;
			int sizeToDistribute = 0;
			System.out.println("isBackup");
			mod = newSignals.size() % (membersQty - 1);
			sizeToDistribute = mod == 0 ? newSignals.size() / (membersQty - 1)
					: (newSignals.size() + mod) / (membersQty - 1);
			Address myAddress = connection.getMyAddress();
			int random = 0;
			List<Address> chosen = new ArrayList<Address>();
			// Can't send the backups to me, because the signals are
			// mine
			chosen.add(myAddress);
			int cicles = membersQty - 1;
			for (int i = 0; i < cicles; i++) {
				while (chosen
						.contains(members.get(random = random(membersQty))))
					;
				Address futureOwner = members.get(random);
				chosen.add(futureOwner);
				List<Signal> auxList = new ArrayList<Signal>();
				if (i == cicles - 1) {
					sizeToDistribute = newSignals.size();
				}
				newSignals.drainTo(auxList, sizeToDistribute);
				if (!futureOwner.equals(myAddress)) {
					List<Backup> backupList = new ArrayList<Backup>();
					// Distribute those backups that were mine and got lost
					// with the node fallen
					for (Signal s : auxList) {
						backupList
								.add(new Backup(connection.getMyAddress(), s));
					}
					if (!backupList.isEmpty()) {
						this.sendBackups.putAll(futureOwner, backupList);
						if(fallenNodeAddress == null){
							System.out.println("fallen es null 2");
						}
						connection
								.sendMessageTo(futureOwner, new SignalMessage(
										connection.getMyAddress(),
										fallenNodeAddress,
										SignalMessageType.BACK_UPS, backupList));
						ret += backupList.size();
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
	 * When a node falls, the signals that it had are redistributed to the other
	 * nodes as new signals.
	 * 
	 * @param fallenNodeAddress
	 */
	protected int distributeNewSignalsFromBackups(Address fallenNodeAddress) {
		int membersQty = 1;
		List<Address> members = null;
		Semaphore sem = new Semaphore(0);
		tasksDone.put(fallenNodeAddress, sem);
		System.out.println("semaforo put: "
				+ tasksDone.get(fallenNodeAddress).availablePermits());
		if (connection != null) {
			members = connection.getMembers();
			membersQty = members.size();
		}
		BlockingQueue<Signal> newSignals = new LinkedBlockingQueue<Signal>(
				backups.get(fallenNodeAddress));
		// System.out.println("tamaño posta: " + newSignals.size());
		if (newSignals.isEmpty()) {
			return 0;
		}
		if (membersQty != 1) {
			int mod = 0;
			int sizeToDistribute = 0;
			System.out.println("!isBackup");
			mod = newSignals.size() % membersQty;
			sizeToDistribute = mod == 0 ? newSignals.size() / membersQty
					: (newSignals.size() + mod) / membersQty;
			Address myAddress = connection.getMyAddress();
			int random = 0;
			List<Address> chosen = new ArrayList<Address>();
			int cicles = membersQty;
			int ret = 0;
			for (int i = 0; i < cicles; i++) {
				while (chosen
						.contains(members.get(random = random(membersQty))))
					;
				Address futureOwner = members.get(random);
				chosen.add(futureOwner);
				List<Signal> auxList = new ArrayList<Signal>();
				if (i == cicles - 1) {
					sizeToDistribute = newSignals.size();
				}
				newSignals.drainTo(auxList, sizeToDistribute);

				if (!futureOwner.equals(myAddress)) {
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
				} else {
					this.signals.addAll(auxList);
					System.out.println("asigno a mi");
					for (Signal signal : auxList) {
						distributeBackup(connection.getMyAddress(),
								fallenNodeAddress, signal);
					}

				}
				ret += auxList.size();
				// Wait for all the messages to arrive
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
			return 0;
		}
	}

	//
	// /**
	// * Distributes signals/backups according to the quantity of nodes
	// * (randomly).
	// *
	// * @param address
	// * @param backups
	// * @param isBackup
	// * Is backup or signal
	// */
	// private void distributeBackups(Address address,
	// Multimap<Address, Signal> backups, boolean isBackup) {
	// int membersQty = 1;
	// List<Address> members = null;
	// if (connection != null) {
	// members = connection.getMembers();
	// membersQty = members.size();
	// }
	// BlockingQueue<Signal> newSignals = new LinkedBlockingQueue<Signal>(
	// backups.get(address));
	// // System.out.println("tamaño posta: " + newSignals.size());
	// if (newSignals.isEmpty()) {
	// return;
	// }
	// if (membersQty != 1) {
	// int mod = 0;
	// int sizeToDistribute = 0;
	// if (isBackup) {
	// System.out.println("isBackup");
	// mod = newSignals.size() % (membersQty - 1);
	// sizeToDistribute = mod == 0 ? newSignals.size()
	// / (membersQty - 1) : (newSignals.size() + mod)
	// / (membersQty - 1);
	// } else {
	// System.out.println("!isBackup");
	// mod = newSignals.size() % membersQty;
	// sizeToDistribute = mod == 0 ? newSignals.size() / membersQty
	// : (newSignals.size() + mod) / membersQty;
	// }
	// Address myAddress = connection.getMyAddress();
	// int random = 0;
	// List<Address> chosen = new ArrayList<Address>();
	// if (isBackup) {
	// // Can't send the backups to me, because the signals are
	// // mine
	// chosen.add(myAddress);
	// }
	// int cicles = isBackup ? membersQty - 1 : membersQty;
	// for (int i = 0; i < cicles; i++) {
	// while (chosen
	// .contains(members.get(random = random(membersQty))))
	// ;
	// Address futureOwner = members.get(random);
	// chosen.add(futureOwner);
	// List<Signal> auxList = new ArrayList<Signal>();
	// if (i == cicles - 1) {
	// sizeToDistribute = newSignals.size();
	// }
	// newSignals.drainTo(auxList, sizeToDistribute);
	// if (!futureOwner.equals(myAddress)) {
	// if (isBackup) {
	// List<Backup> backupList = new ArrayList<Backup>();
	// // Distribute those backups that were mine and got lost
	// // with the node fallen
	// for (Signal s : auxList) {
	// backupList.add(new Backup(
	// connection.getMyAddress(), s));
	// }
	// if (!backupList.isEmpty()) {
	// this.sendBackups.putAll(futureOwner, backupList);
	// connection.sendMessageTo(futureOwner,
	// new SignalMessage(
	// connection.getMyAddress(),
	// SignalMessageType.BACK_UPS,
	// backupList));
	// }
	// } else {
	// if (!auxList.isEmpty()) {
	// this.sendSignals.putAll(futureOwner, auxList);
	// connection
	// .sendMessageTo(
	// futureOwner,
	// new SignalMessage(
	// connection.getMyAddress(),
	// auxList,
	// SignalMessageType.GENERATE_NEW_SIGNALS_FROM_BACKUP));
	// }
	// }
	// } else {
	// if (isBackup) {
	// // Won't happen if there were 2 members or more
	// System.out.println("no deberia pasar");
	// } else {
	// this.signals.addAll(auxList);
	// for (Signal signal : auxList) {
	// distributeBackup(connection.getMyAddress(), signal);
	// }
	// }
	//
	// }
	// }
	// backups.removeAll(address);
	// } else {
	// if (isBackup) {
	// // do nothing
	// } else {
	// this.signals.addAll(newSignals);
	// if (connection != null) {
	// this.backups.removeAll(address);
	// this.backups.putAll(connection.getMyAddress(), newSignals);
	// }
	// backups.putAll(connection.getMyAddress(),
	// this.mySignalsBackup.get(address));
	// this.mySignalsBackup.clear();
	// this.mySignalsBackup.putAll(connection.getMyAddress(),
	// this.signals);
	//
	// // HERE FINISHES
	// }
	// }
	// }

	/**
	 * Distributes randomly a backup from owner.
	 * 
	 * @param owner
	 *            Owner of the signal
	 * @param signal
	 *            Signal
	 */
	protected void distributeBackup(Address owner, Address fallenNodeAddress,
			Signal signal) {
		this.distributeBackup(owner, fallenNodeAddress, signal, null);
	}

	private void distributeBackup(Address signalOwner,
			Address fallenNodeAddress, Signal signal, Backup backup) {
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
				if (backup == null) {
					backup = new Backup(signalOwner, signal);
				}
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
			// Sublist of first sizeToDistribute (if available) signals
			signalsQueue.drainTo(distSignals, sizeToDistribute);

			// If theres nothing to send
			if (!distSignals.isEmpty()) {
				if (!this.sendSignals.putAll(to, distSignals)) {
					System.out
							.println("no deberia pasar agregando a sendSignals");
				}
				connection.sendMessageTo(to,
						new SignalMessage(connection.getMyAddress(),
								distSignals, SignalMessageType.YOUR_SIGNALS));
			}

			// There was only 1 node, so backup must be distributed
			if (membersQty == 2) {
				// Removes all, because then will be added again.
				this.mySignalsBackup.removeAll(connection.getMyAddress());
				for (Signal signal : signalsQueue) {
					// Distribute backup of those signals that are still owned
					// by this node
					distributeBackup(connection.getMyAddress(), null, signal);
				}
				this.backups.get(connection.getMyAddress()).removeAll(
						signalsQueue);
			} else {
				removeWhoBackupMySignal(null, distSignals);
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
	protected void changeBackupOwner(Address oldOwner, Address newOwner,
			List<Signal> signals) {
		// changeBackupOwner(newOwner, signals, this.backups, true, true);
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
	 */
	protected void removeWhoBackupMySignal(Address newOwner,
			List<Signal> signals) {
		for (Address addr : connection.getMembers()) {
			this.mySignalsBackup.get(addr).removeAll(signals);
		}
		// changeBackupOwner(newOwner, signals, this.mySignalsBackup, false,
		// false);
	}

	/**
	 * * Goes throw this.mySignalsBackup changing the owner of the backup
	 * containing signals to newOwner
	 * 
	 * @param address
	 *            New owner of the backup
	 * @param signal
	 */
	protected void changeWhoBackupMySignal(Address from, Address address,
			Address fallenNodeAddress, Signal signal, boolean remove) {
		// List<Signal> list = new ArrayList<Signal>();
		// list.add(signal);
		// changeBackupOwner(address, list, this.mySignalsBackup, false, true);

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
	// @SuppressWarnings("unchecked")
	// protected void changeBackupOwner(Address newOwner, List<Signal> signals,
	// Multimap<Address, Signal> backupMap, boolean changeOwner,
	// boolean addBackup) {
	// Multimap<Address, Signal> toRemove;
	// ArrayListMultimap<Address, Signal> list = ArrayListMultimap.create();
	// toRemove = Multimaps.synchronizedListMultimap(list);
	// List<Signal> indexed = new ArrayList<Signal>();
	// synchronized (backupMap) {
	// for (Address oldOwner : backupMap.keySet()) {
	// for (Signal signal : backupMap.get(oldOwner)) {
	// // TODO: Ver que pasa si hay dos señales iguales dando
	// // vuelta y
	// // hay dos backups tambien
	// if (signals.contains(signal)) {
	// if (!changeOwner && addBackup) {
	// indexed.add(signal);
	// }
	// toRemove.put(oldOwner, signal);
	// if (addBackup) {
	// backupMap.put(newOwner, signal);
	// }
	// }
	// }
	// }
	// }
	// for (Address oldOwner : toRemove.keySet()) {
	// for (Signal signal : toRemove.get(oldOwner)) {
	// if (changeOwner) {
	// // Tell the new owner of the node who has his backups
	// connection.sendMessageTo(newOwner, new SignalMessage(
	// connection.getMyAddress(), signal,
	// SignalMessageType.));
	// // Tell the old owner of the signals who has his backups
	// // connection.sendMessageTo(oldOwner, new SignalMessage(
	// // connection.getMyAddress(), signal,
	// // SignalMessageType.));
	// }
	// backupMap.remove(oldOwner, signal);
	// }
	// }
	//
	// if (!changeOwner && addBackup) {
	// // Those that didn't appeared in the map
	// Collection<Signal> disjunction = CollectionUtils.disjunction(
	// signals, indexed);
	// for (Signal signal : disjunction) {
	// backupMap.put(newOwner, signal);
	// }
	// }
	// }

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
	protected void addSignals(Address from, Address fallenNodeAddress,
			List<Signal> newSignals, String type) {
		this.signals.addAll(newSignals);
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

	/**
	 * Adds a backup to this node and returns ADD_BACKUP_ACK
	 * 
	 * @param from
	 *            Node that send me the order to add the backup
	 * @param backup
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
	 */
	protected void addBackups(Address from, Address fallenNodeAddress,
			List<Backup> backupList) {
		for (Backup backup : backupList) {
			this.backups.put(backup.getAddress(), backup.getSignal());
		}
		if (fallenNodeAddress == null)
			System.out.println("fallen es null");
		connection.sendMessageTo(from,
				new SignalMessage(connection.getMyAddress(), fallenNodeAddress,
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
		Result result = null;
		List<Address> addresses = null;
		if (connection != null) {
			addresses = new ArrayList<Address>(connection.getMembers());
		}
		if (addresses != null && addresses.size() > 1) {
			findingSimilars.set(true);
			// I don't have to send the request to me
			Address myAddress = connection.getMyAddress();
			addresses.remove(myAddress);

			Semaphore semaphore = new Semaphore(0);
			this.requests.put(requestId,
					new FindRequest(addresses, addresses.size(), semaphore));
			connection.broadcastMessage(new SignalMessage(signal, requestId,
					SignalMessageType.FIND_SIMILAR));
			try {
				while (!semaphore.tryAcquire(addresses.size(), 1000,
						TimeUnit.MILLISECONDS)) {
				}
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			FindRequest request = this.requests.get(requestId);
			if (!request.finishedOk()) {
				while (request.finishedDistributing(addresses.size()))
					;
				// y yo no termine..
				this.requests.remove(requestId);
				// get new actual members
				addresses = new ArrayList<Address>(connection.getMembers());
				addresses.remove(myAddress);
				this.requests.put(requestId, new FindRequest(addresses,
						addresses.size(), semaphore));
				try {
					while (!semaphore.tryAcquire(addresses.size(), 1000,
							TimeUnit.MILLISECONDS)) {
					}
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				connection.broadcastMessage(new SignalMessage(signal,
						requestId, SignalMessageType.FIND_SIMILAR));
			}

			result = findSimilarToAux(signal);
			if (request != null) {
				List<Result> results = request.getResults();
				for (Result otherResult : results) {
					for (Item item : otherResult.items()) {
						result = result.include(item);
					}
				}
			}
			findingSimilars.set(false);
		} else {
			result = findSimilarToAux(signal);
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
		System.out.println(connection.getMyAddress() + " findingSimilars...");
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
			// TODO Auto-generated catch block
			e.printStackTrace();

		} catch (ExecutionException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}

	protected boolean isFinding() {
		return findingSimilars.get();
	}

	public int getReceivedSignals() {
		return receivedSignals.get();
	}
}