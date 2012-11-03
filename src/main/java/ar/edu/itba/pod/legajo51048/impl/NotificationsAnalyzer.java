package ar.edu.itba.pod.legajo51048.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingDeque;
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
	private final MultithreadedSignalProcessor processor;
	private final Multimap<Address, Signal> backups;
	private final ConcurrentMap<Integer, FindRequest> requests;
	private final BlockingQueue<Signal> signals;
	private Connection connection;
	private Semaphore waitDistributionSemaphore;
	private Semaphore waitReadyForDistributionSemaphore;

	public NotificationsAnalyzer(BlockingQueue<Signal> signals,
			BlockingQueue<SignalMessage> notifications,
			MultithreadedSignalProcessor processor,
			ConcurrentMap<Integer, FindRequest> requests,

			Multimap<Address, Signal> backups, Connection connection,
			Semaphore semaphore1, Semaphore semaphore2) {
		this.signals = signals;
		this.notifications = notifications;
		this.processor = processor;
		this.requests = requests;
		this.connection = connection;
		this.backups = backups;
		this.waitDistributionSemaphore = semaphore1;
		this.waitReadyForDistributionSemaphore = semaphore2;
	}

	public void finish() {
		finishedAnalyzer.set(true);
	}

	@Override
	public void run() {
		while (!finishedAnalyzer.get()) {
			try {
				SignalMessage notification;
				notification = notifications.take();

				switch (notification.getType()) {

				case SignalMessageType.REQUEST_NOTIFICATION:
					FindRequest request = requests.get(notification
							.getRequestId());
					request.addResult(notification.getResult(),
							notification.getAddress());
					break;

				case SignalMessageType.NEW_NODE:
					processor.distributeSignals(notification.getAddress());
					break;

				case SignalMessageType.BYE_NODE:

					Address fallenNodeAddress = notification.getAddress();

					// Signalibute lost signals.
					BlockingQueue<Signal> toDistribute = new LinkedBlockingDeque<Signal>();
					synchronized (signals) {
						System.out.println("signals size:"
								+ this.signals.size());
						toDistribute.addAll(this.signals);
						this.signals.clear();
					}
					synchronized (backups) {
						System.out.println("backups size:"
								+ this.backups.get(fallenNodeAddress).size());
						toDistribute.addAll(backups.get(fallenNodeAddress));
						this.backups.clear();
					}
					connection.broadcastMessage(new SignalMessage(connection
							.getMyAddress(),
							SignalMessageType.READY_FOR_REDISTRIBUTION));
					
					while (!waitReadyForDistributionSemaphore.tryAcquire(
							connection.getMembersQty() - 1, 1000,
							TimeUnit.MILLISECONDS)) {
						System.out
								.println("while de READY_FOR_REDISTRIBUTION: "
										+ waitReadyForDistributionSemaphore
												.availablePermits());
					}
					System.out.println("todos terminan de distribuir");
					waitReadyForDistributionSemaphore.drainPermits();
					System.out.println("to distribute: " + toDistribute.size());
					processor.distributeSignals(toDistribute);
					System.out.println("finished distributing");
					// Tell everyone that distribution finished.
					connection.broadcastMessage(new SignalMessage(connection
							.getMyAddress(), fallenNodeAddress,
							SignalMessageType.FINISHED_REDISTRIBUTION));

					// Wait for all the nodes to distribute
					while (!waitDistributionSemaphore.tryAcquire(
							connection.getMembersQty() - 1, 1000,
							TimeUnit.MILLISECONDS)) {
						System.out.println("while de FINISHED_REDISTRIBUTION: "
								+ waitDistributionSemaphore.availablePermits());
					}
					waitDistributionSemaphore.drainPermits();
					System.out.println("finished recovery");

					// Find requests that had aborted cause the node fell
					for (FindRequest r : requests.values()) {
						if (r.getAddresses().contains(fallenNodeAddress)) {
							List<Address> addresses = new ArrayList<Address>(
									connection.getMembers());
							r.restart(addresses);
							connection.broadcastMessage(new SignalMessage(r
									.getSignal(), r.getRequestId(),
									SignalMessageType.FIND_SIMILAR));

						}
					}

					break;
				}
			} catch (InterruptedException e) {
			}
		}
	}

}
