package ar.edu.itba.pod.legajo51048.impl;

import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import ar.edu.itba.pod.api.NodeStats;
import ar.edu.itba.pod.api.Result;
import ar.edu.itba.pod.api.SPNode;
import ar.edu.itba.pod.api.Signal;
import ar.edu.itba.pod.api.SignalProcessor;
import ar.edu.itba.pod.api.Result.Item;

public class MultithreadedSignalProcessor implements SPNode, SignalProcessor {

	private final BlockingQueue<Signal> signals;
	private final ExecutorService executor;
	private final int threadsQty;

	private AtomicInteger receivedSignals = new AtomicInteger(0);
	private String cluster = null;

	public MultithreadedSignalProcessor(int threadsQty) {
		this.signals = new LinkedBlockingQueue<Signal>();
		this.executor = Executors.newFixedThreadPool(threadsQty);
		this.threadsQty = threadsQty;
	}

	@Override
	public void join(String clusterName) throws RemoteException {
		if (cluster != null) {
			throw new IllegalStateException("Already in cluster " + cluster);
		}
		if (!signals.isEmpty()) {
			throw new IllegalStateException(
					"Can't join a cluster because there are signals already stored");
		}
		this.cluster = clusterName;

	}

	@Override
	public void exit() throws RemoteException {
		signals.clear();
		receivedSignals = new AtomicInteger(0);
		cluster = null;
	}

	@Override
	public NodeStats getStats() throws RemoteException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void add(Signal signal) throws RemoteException {
		signals.add(signal);

	}

	@Override
	public Result findSimilarTo(Signal signal) throws RemoteException {
		if (signal == null) {
			throw new IllegalArgumentException("Signal cannot be null");
		}

		List<Callable<Result>> tasks = new ArrayList<Callable<Result>>();
		try {
			for (int i = 0; i < threadsQty; i++) {
				tasks.add(new Worker(signal, signals));
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

}
