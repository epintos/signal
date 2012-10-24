package ar.edu.itba.pod.legajo51048.impl;

import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import org.jgroups.Address;

import ar.edu.itba.pod.api.NodeStats;
import ar.edu.itba.pod.api.Result;
import ar.edu.itba.pod.api.Result.Item;
import ar.edu.itba.pod.api.SPNode;
import ar.edu.itba.pod.api.Signal;
import ar.edu.itba.pod.api.SignalProcessor;

public class MultithreadedSignalProcessor implements SPNode, SignalProcessor {

	private final BlockingQueue<Signal> signals;
	private final ExecutorService executor;
	private final int threadsQty;

	private AtomicInteger receivedSignals = new AtomicInteger(0);
	private Connection connection = null;

	public MultithreadedSignalProcessor(int threadsQty) {
		this.signals = new LinkedBlockingQueue<Signal>();
		this.executor = Executors.newFixedThreadPool(threadsQty);
		this.threadsQty = threadsQty;
	}

	@Override
	public void join(String clusterName) throws RemoteException {
		if (connection != null) {
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
		connection = null;
	}

	@Override
	public NodeStats getStats() throws RemoteException {
		return new NodeStats("cluster " + connection.getClusterName(),
				receivedSignals.longValue(), signals.size(), 0, true);
	}

	@Override
	public void add(Signal signal) throws RemoteException {
		Set<Address> users = connection.getUsers();
		Random random = new Random();
		int r = random.nextInt(users.size());
		int i = 0;
		for (Address address : users) {
			if (i++ == r) {
				connection.sendMessageTo(address, signal);
			}
		}
	}

	protected void addSignal(Signal signal) {
		this.signals.add(signal);
	}

	protected void addBackup(Backup backup) {

	}

	@Override
	public Result findSimilarTo(Signal signal) throws RemoteException {
		if (signal == null) {
			throw new IllegalArgumentException("Signal cannot be null");
		}

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
