package ar.edu.itba.pod.legajo51048.impl;

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
	private Semaphore waitFallenDistributionSemaphore;
	private Semaphore waitReadyForFallenDistributionSemaphore;
	private Semaphore waitNewDistributionSemaphore;
	private Logger logger;

	public NotificationsAnalyzer(BlockingQueue<Signal> signals,
			BlockingQueue<SignalMessage> notifications,
			MultithreadedSignalProcessor processor,
			ConcurrentMap<Integer, FindRequest> requests,

			Multimap<Address, Signal> backups, Connection connection,
			Semaphore semaphore1, Semaphore semaphore2, Semaphore semaphore3) {
		this.signals = signals;
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
		int qty = connection.getMembersQty() - 1;
		try {
			while (!waitNewDistributionSemaphore.tryAcquire(qty, 3000,
					TimeUnit.MILLISECONDS)) {
				logger.info("New node waiting for "
						+ waitNewDistributionSemaphore.availablePermits()
						+ " nodes to finish new node distribution ");
			}
		} catch (InterruptedException e1) {
		}
		waitNewDistributionSemaphore.drainPermits();
		logger.info("New node ready");
		while (!finishedAnalyzer.get()) {
			try {
				SignalMessage notification;
				notification = notifications.take();

				switch (notification.getType()) {

				case SignalMessageType.FIND_SIMILAR:
					processor.findMySimilars(notification.getAddress(),
							notification.getSignal(), notification.getNumber(),
							notification.getTimestamp());
					break;
				case SignalMessageType.NEW_NODE:
					processor.setDegradedMode(true);
					qty = notification.getNumber() - 2;
					logger.info("New node detected. Starting distribution...");
					processor.distributeSignals(notification.getAddress());
					logger.info("Finished new node distribution.");
					connection
							.broadcastMessage(new SignalMessage(
									connection.getMyAddress(),
									notification.getAddress(),
									SignalMessageType.FINISHED_NEW_NODE_REDISTRIBUTION));
					while (!waitNewDistributionSemaphore.tryAcquire(qty, 3000,
							TimeUnit.MILLISECONDS)) {
						logger.info("Waiting for "
								+ waitNewDistributionSemaphore
										.availablePermits()
								+ " nodes to finish new node distribution ");
					}
					logger.info("System recovered from new node");
					processor.setDegradedMode(false);
					break;

				case SignalMessageType.BYE_NODE:
					processor.setDegradedMode(true);
					Address fallenNodeAddress = notification.getAddress();
					logger.info("Fallen " + fallenNodeAddress
							+ " node detected. Preparing distribution...");
					BlockingQueue<Signal> toDistribute = new LinkedBlockingDeque<Signal>();
					synchronized (signals) {
						toDistribute.addAll(this.signals);
						this.signals.clear();
					}
					synchronized (backups) {
						toDistribute.addAll(backups.get(fallenNodeAddress));
						this.backups.clear();
					}
					connection
							.broadcastMessage(new SignalMessage(
									connection.getMyAddress(),
									SignalMessageType.READY_FOR_FALLEN_NODE_REDISTRIBUTION));

					while (!waitReadyForFallenDistributionSemaphore.tryAcquire(
							connection.getMembersQty() - 1, 5000,
							TimeUnit.MILLISECONDS)) {
						logger.info("Waiting for "
								+ waitReadyForFallenDistributionSemaphore
										.availablePermits()
								+ " nodes to be ready for fallen node distribution");
					}
					logger.info("System ready for fallen node distribution. Starting...");
					waitReadyForFallenDistributionSemaphore.drainPermits();
					processor.distributeSignals(toDistribute);
					logger.info("Finished fallen node distribution");

					// Tell everyone that distribution finished.
					connection
							.broadcastMessage(new SignalMessage(
									connection.getMyAddress(),
									SignalMessageType.FINISHED_FALLEN_NODE_REDISTRIBUTION));

					// Wait for all the nodes to distribute
					while (!waitFallenDistributionSemaphore.tryAcquire(
							connection.getMembersQty() - 1, 3000,
							TimeUnit.MILLISECONDS)) {
						logger.info("Waiting for "
								+ waitFallenDistributionSemaphore
										.availablePermits()
								+ " nodes to finish fallen node redistribution");
					}
					waitFallenDistributionSemaphore.drainPermits();
					logger.info("System recovered from node fallen");

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
