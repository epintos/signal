package ar.edu.itba.pod.legajo51048.workers;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.log4j.Logger;
import org.jgroups.Address;

import ar.edu.itba.pod.api.Signal;
import ar.edu.itba.pod.legajo51048.Connection;
import ar.edu.itba.pod.legajo51048.impl.MultithreadedSignalProcessor;
import ar.edu.itba.pod.legajo51048.messages.FindRequest;
import ar.edu.itba.pod.legajo51048.messages.SignalMessage;
import ar.edu.itba.pod.legajo51048.messages.SignalMessageType;

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
	private final BlockingQueue<Address> members;
	private Connection connection;

	// Semaphore to wait for everyone to be ready for distributing when a node
	// fell
	private final Semaphore waitReadyForFallenDistributionSemaphore;

	// Semaphore to wait for everyone to finish distributing when a node
	// fell
	private final Semaphore waitFallenDistributionSemaphore;

	// Semaphore to wait for everyone to finish distributing when a new node
	// joins
	private final Semaphore waitNewDistributionSemaphore;

	private Logger logger;

	public NotificationsAnalyzer(BlockingQueue<Signal> signals,
			BlockingQueue<SignalMessage> notifications,
			MultithreadedSignalProcessor processor,
			ConcurrentMap<Integer, FindRequest> requests,

			Multimap<Address, Signal> backups, Connection connection,
			BlockingQueue<Address> members, Semaphore semaphore1,
			Semaphore semaphore2, Semaphore semaphore3) {
		this.signals = signals;
		this.members = members;
		this.notifications = notifications;
		this.processor = processor;
		this.requests = requests;
		this.connection = connection;
		this.backups = backups;
		this.waitFallenDistributionSemaphore = semaphore1;
		this.waitReadyForFallenDistributionSemaphore = semaphore2;
		this.waitNewDistributionSemaphore = semaphore3;
		logger = Logger.getLogger("NotificationsAnalyzer");
	}

	public void finish() {
		finishedAnalyzer.set(true);
	}

	@Override
	public void run() {
		processor.setDegradedMode(true);

		// Don't wait for this node ack
		int qty = members.size() - 1;
		try {
			while (!waitNewDistributionSemaphore.tryAcquire(qty, 3000,
					TimeUnit.MILLISECONDS)) {
				logger.debug("New node waiting for "
						+ waitNewDistributionSemaphore.availablePermits()
						+ " nodes to finish new node distribution ");
			}
		} catch (InterruptedException e1) {
		}
		waitNewDistributionSemaphore.drainPermits();
		logger.debug("New node ready");
		processor.setDegradedMode(false);

		while (!finishedAnalyzer.get()) {
			try {
				SignalMessage notification;
				notification = notifications.take();

				switch (notification.getType()) {

				case SignalMessageType.FIND_SIMILAR:
					processor.findMySimilars(notification.getAddress(),
							notification.getSignal(),
							notification.getRequestId(),
							notification.getTimestamp());
					break;
				case SignalMessageType.NEW_NODE:
					processor.setDegradedMode(true);
					this.members.put(notification.getAddress());

					// Don't wait for this node and the new one acks
					qty = this.members.size() - 2;
					logger.debug("New node detected. Starting distribution...");
					processor.distributeSignals(notification.getAddress());
					logger.debug("Finished new node distribution.");
					for (Address addr : this.members) {
						connection
								.sendMessageTo(
										addr,
										new SignalMessage(
												connection.getMyAddress(),
												SignalMessageType.FINISHED_NEW_NODE_REDISTRIBUTION));
					}
					while (!waitNewDistributionSemaphore.tryAcquire(qty, 3000,
							TimeUnit.MILLISECONDS)) {
						logger.debug("Waiting for "
								+ waitNewDistributionSemaphore
										.availablePermits()
								+ " nodes to finish new node distribution ");
					}
					logger.debug("System recovered from new node");
					processor.setDegradedMode(false);
					break;

				case SignalMessageType.BYE_NODE:
					processor.setDegradedMode(true);
					Address fallenNodeAddress = notification.getAddress();
					this.members.remove(fallenNodeAddress);
					qty = members.size();
					logger.debug("Fallen " + fallenNodeAddress
							+ " node detected. Preparing distribution...");

					BlockingQueue<Signal> toDistribute = new LinkedBlockingDeque<Signal>();
					toDistribute.addAll(this.signals);
					this.signals.clear();

					toDistribute.addAll(backups.get(fallenNodeAddress));
					this.backups.clear();
					connection
							.broadcastMessage(new SignalMessage(
									connection.getMyAddress(),
									SignalMessageType.READY_FOR_FALLEN_NODE_REDISTRIBUTION));

					while (!waitReadyForFallenDistributionSemaphore.tryAcquire(
							qty - 1, 5000, TimeUnit.MILLISECONDS)) {
						logger.debug("Waiting for "
								+ waitReadyForFallenDistributionSemaphore
										.availablePermits()
								+ " nodes to be ready for fallen node distribution");
					}
					logger.debug("System ready for fallen node distribution. Starting...");
					waitReadyForFallenDistributionSemaphore.drainPermits();
					processor.distributeSignals(toDistribute);
					logger.debug("Finished fallen node distribution");

					// Tell everyone that this node distribution finished.
					connection
							.broadcastMessage(new SignalMessage(
									connection.getMyAddress(),
									SignalMessageType.FINISHED_FALLEN_NODE_REDISTRIBUTION));

					// Wait for all the nodes to distribute
					while (!waitFallenDistributionSemaphore.tryAcquire(qty - 1,
							3000, TimeUnit.MILLISECONDS)) {
						logger.debug("Waiting for "
								+ waitFallenDistributionSemaphore
										.availablePermits()
								+ " nodes to finish fallen node redistribution");
					}
					waitFallenDistributionSemaphore.drainPermits();
					logger.debug("System recovered from node fallen");

					// Find requests that had aborted cause a node fell.
					// Timestamp is added because old results may arrive, so
					// they are discarted
					for (FindRequest r : requests.values()) {
						if (r.getAddresses().contains(fallenNodeAddress)) {
							List<Address> addresses = new ArrayList<Address>(
									connection.getMembers());
							long timestamp = System.currentTimeMillis();
							r.restart(addresses, timestamp);
							connection.broadcastMessage(new SignalMessage(r
									.getSignal(), connection.getMyAddress(), r
									.getRequestId(),
									System.currentTimeMillis(),
									SignalMessageType.FIND_SIMILAR));

						}
					}
					processor.setDegradedMode(false);
					break;
				}
			} catch (InterruptedException e) {
			}
		}
	}
}
