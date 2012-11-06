package ar.edu.itba.pod.legajo51048.impl;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.log4j.Logger;
import org.jgroups.Address;

import ar.edu.itba.pod.api.Signal;

public class AcknowledgesAnalyzer extends Thread {
	private AtomicBoolean finishedAnalyzer = new AtomicBoolean(false);
	private final BlockingQueue<SignalMessage> acknowledges;
	private final BlockingQueue<Signal> sendSignals;
	private final MultithreadedSignalProcessor processor;
	private final BlockingQueue<Backup> sendBackups;
	private final ConcurrentMap<Integer, FindRequest> requests;
	private Connection connection;
	private Semaphore waitReadyForFallenDistributionSemaphore;
	private Semaphore waitFallenDistributionSemaphore;
	private Semaphore waitNewDistributionSemaphore;
	private Logger logger;

	public AcknowledgesAnalyzer(BlockingQueue<SignalMessage> acknowledges,
			BlockingQueue<Signal> sendSignals,
			MultithreadedSignalProcessor processor,
			BlockingQueue<Backup> sendBackups,
			ConcurrentMap<Integer, FindRequest> requests,
			Connection connection, Semaphore semaphore1, Semaphore semaphore2,
			Semaphore semaphore3) {
		this.acknowledges = acknowledges;
		this.sendSignals = sendSignals;
		this.processor = processor;
		this.requests = requests;
		this.sendBackups = sendBackups;
		this.connection = connection;
		this.waitFallenDistributionSemaphore = semaphore1;
		this.waitReadyForFallenDistributionSemaphore = semaphore2;
		this.waitNewDistributionSemaphore = semaphore3;
		this.logger = Logger.getLogger("AcknowledgesAnalyzer");
	}

	public void finish() {
		finishedAnalyzer.set(true);
	}

	@Override
	public void run() {
		while (!finishedAnalyzer.get()) {
			try {
				SignalMessage acknowledge;
				acknowledge = acknowledges.take();

				switch (acknowledge.getType()) {
				
				case SignalMessageType.REQUEST_NOTIFICATION:
					FindRequest request = requests.get(acknowledge
							.getRequestId());
					request.addResult(acknowledge.getResult(),
							acknowledge.getAddress(),
							acknowledge.getTimestamp());
					break;
				case SignalMessageType.ADD_SIGNAL_ACK:
					if (!sendSignals.add(acknowledge.getSignal())) {
						logger.warn("esto no deberia pasar "
								+ SignalMessageType.ADD_SIGNAL_ACK);
					}
					break;

				case SignalMessageType.ADD_SIGNALS_ACK:
					if (!sendSignals.addAll(acknowledge.getSignals())) {
						logger.warn("esto no deberia pasar "
								+ SignalMessageType.ADD_SIGNALS_ACK);
					}
					// Tell everyone that some backup owners have changed
					Address signalOwner = acknowledge.getAddress();
					connection.broadcastMessage(new SignalMessage(signalOwner,
							acknowledge.getSignals(),
							SignalMessageType.CHANGE_SIGNALS_OWNER));
					break;

				case SignalMessageType.GENERATE_NEW_SIGNALS_FROM_BACKUP_ACK:
					for (Signal s : acknowledge.getSignals()) {
						if (!sendSignals.add(s)) {
							logger.warn("esto no deberia pasar "
									+ SignalMessageType.GENERATE_NEW_SIGNALS_FROM_BACKUP_ACK);
						}
						processor.distributeBackup(acknowledge.getAddress(), s);
					}
					break;

				case SignalMessageType.ADD_BACKUP_ACK:
					if (!sendBackups.add(acknowledge.getBackup())) {
						logger.warn("no deberia pasar "
								+ SignalMessageType.ADD_BACKUP_ACK);
					}

					break;

				case SignalMessageType.ADD_BACKUPS_ACK:
					if (!sendBackups.addAll(acknowledge.getBackupList())) {
						logger.warn("no deberia pasar "
								+ SignalMessageType.ADD_BACKUPS_ACK);
					}
					break;

				case SignalMessageType.FINISHED_FALLEN_NODE_REDISTRIBUTION:
					waitFallenDistributionSemaphore.release();
					break;
				case SignalMessageType.READY_FOR_FALLEN_NODE_REDISTRIBUTION:
					waitReadyForFallenDistributionSemaphore.release();
					break;
				case SignalMessageType.FINISHED_NEW_NODE_REDISTRIBUTION:
					waitNewDistributionSemaphore.release();
					break;

				}
			} catch (InterruptedException e) {
			}
		}
	}

}
