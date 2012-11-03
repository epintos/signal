package ar.edu.itba.pod.legajo51048.impl;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;

import org.jgroups.Address;

import ar.edu.itba.pod.api.Signal;

public class AcknowledgesAnalyzer extends Thread {
	private AtomicBoolean finishedAnalyzer = new AtomicBoolean(false);
	private final BlockingQueue<SignalMessage> acknowledges;
	private final BlockingQueue<Signal> sendSignals;
	private final MultithreadedSignalProcessor processor;
	private final BlockingQueue<Backup> sendBackups;
	private Connection connection;
	private Semaphore waitDistributionSemaphore;
	private Semaphore waitReadyForDistributionSemaphore;

	public AcknowledgesAnalyzer(BlockingQueue<SignalMessage> acknowledges,
			BlockingQueue<Signal> sendSignals,
			MultithreadedSignalProcessor processor,
			BlockingQueue<Backup> sendBackups,
			ConcurrentMap<Integer, FindRequest> requests,
			Connection connection, Semaphore semaphore1, Semaphore semaphore2) {
		this.acknowledges = acknowledges;
		this.sendSignals = sendSignals;
		this.processor = processor;
		this.sendBackups = sendBackups;
		this.connection = connection;
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
				SignalMessage acknowledge;
				acknowledge = acknowledges.take();

				switch (acknowledge.getType()) {

				case SignalMessageType.ADD_SIGNAL_ACK:
					if (!sendSignals.remove(acknowledge.getSignal())) {
						System.out.println("esto no deberia pasar "
								+ SignalMessageType.ADD_SIGNAL_ACK);
					}
					break;

				case SignalMessageType.ADD_SIGNALS_ACK:
					if (!sendSignals.removeAll(acknowledge.getSignals())) {
						System.out.println("esto no deberia pasar "
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
						if (!sendSignals.remove(s)) {
							System.out
									.println("esto no deberia pasar "
											+ SignalMessageType.GENERATE_NEW_SIGNALS_FROM_BACKUP_ACK);
						}
						processor.distributeBackup(acknowledge.getAddress(), s);
					}
					break;

				case SignalMessageType.ADD_BACKUP_ACK:
					if (!sendBackups.remove(acknowledge.getBackup())) {
						System.out.println("no deberia pasar "
								+ SignalMessageType.ADD_BACKUP_ACK);
					}

					break;

				case SignalMessageType.ADD_BACKUPS_ACK:
					if (!sendBackups.removeAll(acknowledge.getBackupList())) {
						System.out.println("no deberia pasar "
								+ SignalMessageType.ADD_BACKUPS_ACK);
					}
					break;


				case SignalMessageType.FINISHED_REDISTRIBUTION:
					waitDistributionSemaphore.release();
					break;
				case SignalMessageType.READY_FOR_REDISTRIBUTION:
					waitReadyForDistributionSemaphore.release();
					break;

				}
			} catch (InterruptedException e) {
			}
		}
	}

}
