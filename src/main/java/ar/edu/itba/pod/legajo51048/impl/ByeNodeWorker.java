package ar.edu.itba.pod.legajo51048.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import org.jgroups.Address;

import com.google.common.collect.Multimap;

import ar.edu.itba.pod.api.Signal;

/**
 * Worker that distributes signals and backups when a node falls. It also re
 * sends find similar requests if needed.
 * 
 * @author Esteban G. Pintos
 * 
 */
public class ByeNodeWorker extends Thread {

	private final MultithreadedSignalProcessor processor;
	private final ConcurrentMap<Address, Semaphore> tasksDone;
	private final ConcurrentMap<Address, Semaphore> semaphores;
	private final ConcurrentMap<Integer, FindRequest> requests;
	private final SignalMessage notification;
	private Connection connection;
	private final Multimap<Address, Signal> backups;
	private final BlockingQueue<Signal> signals;

	public ByeNodeWorker(SignalMessage notification,
			MultithreadedSignalProcessor processor,
			ConcurrentMap<Address, Semaphore> tasksDone,
			ConcurrentMap<Address, Semaphore> semaphores,
			Connection connection,
			ConcurrentMap<Integer, FindRequest> requests,
			Multimap<Address, Signal> backups, BlockingQueue<Signal> signals) {
		this.processor = processor;
		this.requests = requests;
		this.tasksDone = tasksDone;
		this.notification = notification;
		this.semaphores = semaphores;
		this.connection = connection;
		this.backups = backups;
		this.signals = signals;
	}

	@Override
	public void run() {
		try {
			Address fallenNodeAddress = notification.getAddress();

			// Signalibute lost signals.
			BlockingQueue<Signal> toDistribute = new LinkedBlockingDeque<Signal>();
			toDistribute.addAll(this.signals);
			toDistribute.addAll(backups.get(fallenNodeAddress));
//			processor.distributeNewSignalsFromBackups(toDistribute);
			processor.distributeSignals(toDistribute);

//			Semaphore sem = tasksDone.get(fallenNodeAddress);
//			while (!sem.tryAcquire(tasks, 1, TimeUnit.SECONDS)) {
//				System.out.println("en el 1er while " + sem.availablePermits());
//			}
//
//			// Distribute lost backups.
//			tasks = processor.distributeLostBackups(notification.getAddress());
//			sem = tasksDone.get(fallenNodeAddress);
//			while (!sem.tryAcquire(tasks, 1, TimeUnit.SECONDS)) {
//				System.out.println("en el 2do while " + sem.availablePermits());
//			}
//			tasksDone.remove(fallenNodeAddress);

			System.out.println("finished distributing");
			// Tell everyone that distribution finished.
			connection.broadcastMessage(new SignalMessage(connection
					.getMyAddress(), fallenNodeAddress,
					SignalMessageType.FINISHED_REDISTRIBUTION));

			// Wait for all the nodes to distribute
			Semaphore semaphore = null;
			synchronized (semaphores) {
				semaphore = semaphores.get(fallenNodeAddress);
				if (semaphore == null) {
					semaphore = new Semaphore(0);
					semaphores.put(fallenNodeAddress, semaphore);
				}
			}
			while (!semaphore.tryAcquire(connection.getMembersQty() - 1, 1000,
					TimeUnit.MILLISECONDS)) {
				System.out.println("while de FINISHED_REDISTRIBUTION: "
						+ semaphore.availablePermits());
			}
			semaphores.remove(fallenNodeAddress);
			System.out.println("finished recovery");

			// Find requests that had aborted cause the node fell
			for (FindRequest request : requests.values()) {
				if (request.getAddresses().contains(fallenNodeAddress)) {
					List<Address> addresses = new ArrayList<Address>(
							connection.getMembers());
					request.restart(addresses);
					connection.broadcastMessage(new SignalMessage(request
							.getSignal(), request.getRequestId(),
							SignalMessageType.FIND_SIMILAR));

				}
			}

		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
}
