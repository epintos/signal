package ar.edu.itba.pod.legajo51048.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import org.jgroups.Address;

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

	public ByeNodeWorker(SignalMessage notification,
			MultithreadedSignalProcessor processor,
			ConcurrentMap<Address, Semaphore> tasksDone,
			ConcurrentMap<Address, Semaphore> semaphores,
			Connection connection, ConcurrentMap<Integer, FindRequest> requests) {
		this.processor = processor;
		this.requests = requests;
		this.tasksDone = tasksDone;
		this.notification = notification;
		this.semaphores = semaphores;
		this.connection = connection;
	}

	@Override
	public void run() {
		try {
			Address fallenNodeAddress = notification.getAddress();

			// Distribute lost signals.
			int tasks = processor.distributeNewSignalsFromBackups(notification
					.getAddress());

			Semaphore sem = tasksDone.get(fallenNodeAddress);
			while (!sem.tryAcquire(tasks, 1, TimeUnit.SECONDS)) {
				System.out.println("en el 1er while " + sem.availablePermits());
			}

			// Distribute lost backups.
			tasks = processor.distributeLostBackups(notification.getAddress());
			sem = tasksDone.get(fallenNodeAddress);
			while (!sem.tryAcquire(tasks, 1, TimeUnit.SECONDS)) {
				System.out.println("en el 2do while " + sem.availablePermits());
			}
			tasksDone.remove(fallenNodeAddress);

			System.out.println("finished distributing");
			// Tell everyone that distribution finished.
			connection.broadcastMessage(new SignalMessage(connection
					.getMyAddress(), fallenNodeAddress,
					SignalMessageType.FINISHED_REDISTRIBUTION));

			// Wait for all the nodes to distribute
			Semaphore semaphore = semaphores.get(fallenNodeAddress);
			if (semaphore == null) {
				semaphore = new Semaphore(0);
				semaphores.put(fallenNodeAddress, semaphore);
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
