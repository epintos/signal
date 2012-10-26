package ar.edu.itba.pod.legajo51048.impl;

import java.rmi.RemoteException;
import java.util.ArrayList;
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
	private final Multimap<Address, Signal> backups;
	private final Multimap<Integer, Address> requests;
	private final ExecutorService executor;
	private final int threadsQty;
	public static final String EXIT_MESSAGE = "EXIT_MESSAGE";
	public static final String FIND_SIMILAR = "FIND_SIMILAR";

	private AtomicBoolean degraded = new AtomicBoolean(false);
	private AtomicInteger receivedSignals = new AtomicInteger(0);
	private Connection connection = null;

	public MultithreadedSignalProcessor(int threadsQty) {
		ArrayListMultimap<Address, Signal> list = ArrayListMultimap.create();
		this.backups = Multimaps.synchronizedListMultimap(list);
		ArrayListMultimap<Integer, Address> list2 = ArrayListMultimap.create();
		this.requests = Multimaps.synchronizedListMultimap(list2);
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

	}

	@Override
	public void exit() throws RemoteException {
		signals.clear();
		receivedSignals = new AtomicInteger(0);
		if (connection != null) {
			connection.broadcastMessage(EXIT_MESSAGE);
			connection.disconnect();
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

	private void distributeNewSignal(Signal signal) {
		List<Address> users = connection.getMembers();
		int membersQty = users.size();
		System.out.println("usuarios: " + membersQty);
		int sigRandom = 0;
		int backRandom = 0;
		while ((sigRandom = random(membersQty)) == (backRandom = random(membersQty))
				&& membersQty != 1)
			;
		if (membersQty != 1) {
			Address sigAddress = users.get(sigRandom);
			Address backAddress = users.get(backRandom);
			Address myAddress = connection.getMyAddress();
			if (myAddress.equals(sigAddress)) {
				signals.add(signal);
			} else {
				connection.sendMessageTo(sigAddress, new SignalMessage(signal,
						SignalMessageType.YOUR_SIGNAL));
			}
			if (myAddress.equals(backAddress)) {
				this.backups.put(sigAddress, signal);
			}
			connection.sendMessageTo(backAddress, new SignalMessage(sigAddress,
					signal, SignalMessageType.BACK_UP));
		} else {
			this.signals.add(signal);
			// For easier distribution later
			this.backups.put(connection.getMyAddress(), signal);
		}
	}

	protected void removeBackups(Address address) {
		backups.removeAll(address);
	}

	protected void distributeBackups(Address address) {
		degraded.set(true);
		for (Signal signal : backups.get(address)) {
			// Assign to me
			signals.add(signal);
			List<Address> users = connection.getMembers();
			int limit = users.size();
			boolean me = true;
			Address addr = null;
			while (limit != 1 && me) {
				int to = random(limit);
				addr = connection.getMembers().get(to);
				if (!addr.equals(connection.getMyAddress())) {
					me = false;
				}
			}
			if (limit != 1) {
				distribute(addr, new SignalMessage(connection.getMyAddress(),
						signal, SignalMessageType.BACK_UP));
			}
		}
		backups.removeAll(address);
		degraded.set(false);
	}

	protected void distributeSignals() {
		if (signals.isEmpty()) {
			return;
		}
		degraded.set(true);
		int membersQty = connection.getMembersQty();
		synchronized (signals) {
			int sizeToDistribute = signals.size() / membersQty;
			List<Signal> distSignals = new ArrayList<Signal>();
			signals.drainTo(distSignals, sizeToDistribute);
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
			System.out.println(addr);
			System.out.println("tamaño distSignals: " + distSignals.size());
			// distribute(addr, new SignalMessage(distSignals,
			// SignalMessageType.YOUR_SIGNALS));
			connection.sendMessageTo(addr, new SignalMessage(distSignals,
					SignalMessageType.YOUR_SIGNALS));
			//
			// connection.broadcastMessage(new SignalMessage(addr, distSignals,
			// SignalMessageType.CHANGE_BACK_UP_OWNER));
		}
		degraded.set(false);
	}

	private void distribute(Address address, Object obj) {
		connection.sendMessageTo(address, obj);
	}

	protected void changeBackupOwner(Address address, List<Signal> signals) {
		Multimap<Address, Signal> toRemove;
		ArrayListMultimap<Address, Signal> list = ArrayListMultimap.create();
		toRemove = Multimaps.synchronizedListMultimap(list);
		for (Address addr : backups.keySet()) {
			for (Signal signal : backups.get(addr)) {
				if (signals.contains(signal)) {
					toRemove.put(addr, signal);
					backups.put(address, signal);
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

	protected void addSignal(Signal signal) {
		this.signals.add(signal);
	}

	protected void addSignals(List<Signal> newSignals) {
		System.out.println("agrega signals");
		this.signals.addAll(newSignals);
	}

	protected void addBackup(Address address, Signal signal) {
		this.backups.put(address, signal);
	}

	protected void findMySimilars(Address address, Signal signal) {
		Result result = findSimilarToAux(signal);
		connection.sendMessageTo(address,
				new SignalMessage(connection.getMyAddress(), result,
						SignalMessageType.ASKED_RESULT));
	}

	protected void addResult(Address address, int requestId, Result result) {
		// Bajar en 1 el semaforo
		// Agregar resultado a cola
	}

	@Override
	public Result findSimilarTo(Signal signal) throws RemoteException {
		if (signal == null) {
			throw new IllegalArgumentException("Signal cannot be null");
		}

		// connection.broadcastMessage(new SignalMessage(signal,
		// SignalMessageType.FIND_SIMILAR));
		Semaphore sem = new Semaphore(connection.getMembersQty());
		for (Address address : connection.getMembers()) {
			requests.put(receivedSignals.get(), address);
			connection.sendMessageTo(address, new SignalMessage(signal,
					receivedSignals.get(), SignalMessageType.FIND_SIMILAR));
		}
		try {
			sem.acquire(connection.getMembersQty());
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return findSimilarToAux(signal);
	}

	private Result findSimilarToAux(Signal signal) {
		List<Callable<Result>> tasks = new ArrayList<Callable<Result>>();
		try {
			for (int i = 0; i < threadsQty; i++) {
				// Generate a copy of the signals so they are not lost
				BlockingQueue<Signal> copy = new LinkedBlockingQueue<Signal>(
						signals);
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
			receivedSignals.incrementAndGet();
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

}
