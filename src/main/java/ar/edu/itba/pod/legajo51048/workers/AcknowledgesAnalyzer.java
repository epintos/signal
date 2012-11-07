package ar.edu.itba.pod.legajo51048.workers;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;

import org.jgroups.Address;

import ar.edu.itba.pod.api.Signal;
import ar.edu.itba.pod.legajo51048.Connection;
import ar.edu.itba.pod.legajo51048.messages.Backup;
import ar.edu.itba.pod.legajo51048.messages.FindRequest;
import ar.edu.itba.pod.legajo51048.messages.SignalMessage;
import ar.edu.itba.pod.legajo51048.messages.SignalMessageType;

/**
 * Worker that analyzes the acknowledges/answers of a node
 * 
 * @author Esteban G. Pintos
 * 
 */
public class AcknowledgesAnalyzer extends Thread {
	private AtomicBoolean finishedAnalyzer = new AtomicBoolean(false);
	private final BlockingQueue<SignalMessage> acknowledges;
	private final BlockingQueue<Signal> sendSignals;
	private final BlockingQueue<Backup> sendBackups;
	private final ConcurrentMap<Integer, FindRequest> requests;

	// Semaphore to wait for everyone to be ready for distributing when a node
	// fell
	private final Semaphore waitReadyForFallenDistributionSemaphore;

	// Semaphore to wait for everyone to finish distributing when a node
	// fell
	private final Semaphore waitFallenDistributionSemaphore;

	// Semaphore to wait for everyone to finish distributing when a new node
	// joins
	private final Semaphore waitNewDistributionSemaphore;

	private Connection connection;

	public AcknowledgesAnalyzer(BlockingQueue<SignalMessage> acknowledges,
			BlockingQueue<Signal> sendSignals,
			BlockingQueue<Backup> sendBackups,
			ConcurrentMap<Integer, FindRequest> requests,
			Connection connection, Semaphore semaphore1, Semaphore semaphore2,
			Semaphore semaphore3) {
		this.acknowledges = acknowledges;
		this.sendSignals = sendSignals;
		this.requests = requests;
		this.sendBackups = sendBackups;
		this.connection = connection;
		this.waitFallenDistributionSemaphore = semaphore1;
		this.waitReadyForFallenDistributionSemaphore = semaphore2;
		this.waitNewDistributionSemaphore = semaphore3;
	}

	public void finish() {
		finishedAnalyzer.set(true);
	}

	@Override
	public void run() {
		while (!finishedAnalyzer.get()) {
			try {
				SignalMessage acknowledge = acknowledges.take();

				switch (acknowledge.getType()) {
				case SignalMessageType.FIND_SIMILAR_RESULT:
					FindRequest request = requests.get(acknowledge
							.getRequestId());
					request.addResult(acknowledge.getResult(),
							acknowledge.getAddress(),
							acknowledge.getTimestamp());
					break;

				case SignalMessageType.ADD_SIGNALS_ACK:
					sendSignals.addAll(acknowledge.getSignals());

					// Tell everyone that some backup owners have changed
					Address signalOwner = acknowledge.getAddress();
					connection.broadcastMessage(new SignalMessage(signalOwner,
							acknowledge.getSignals(),
							SignalMessageType.CHANGE_SIGNALS_OWNER));
					break;

				case SignalMessageType.ADD_BACKUPS_ACK:
					sendBackups.addAll(acknowledge.getBackupList());
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
