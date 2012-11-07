package ar.edu.itba.pod.legajo51048.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

import org.jgroups.Address;

import ar.edu.itba.pod.api.Result;
import ar.edu.itba.pod.api.Signal;

/**
 * Class containing a request for finding similar signals.
 * 
 * @author egpintos17
 * 
 */
public class FindRequest {

	// Nodes that are included in the request
	private List<Address> addresses;

	// Quantity of nodes in the request (doesn't include the principal node)
	private AtomicInteger qty;

	private Semaphore semaphore;

	// Results of the request
	private List<Result> results;

	// Signal to analyze
	private Signal signal;

	// Request id
	private int requestId;

	// Request timestamp
	private long timestamp;

	// If true, then a problem happened and the principal node must recalculate
	// his results
	private boolean retry = false;

	private Logger logger;

	public FindRequest(int requestId, Signal signal, List<Address> addresses,
			Semaphore semaphore, long timestamp) {
		this.addresses = addresses;
		this.logger = Logger.getLogger("FindRequest");

		// Remove the principal node
		this.qty = new AtomicInteger(addresses.size() - 1);
		this.requestId = requestId;
		this.signal = signal;
		this.semaphore = semaphore;
		this.results = new ArrayList<Result>();
		this.timestamp = timestamp;
	}

	/**
	 * Adds a result from address. If the timestamp isn't equal to this request
	 * timestamp, then it's an old result that arrived late, so it is discarted.
	 * 
	 * @param result
	 *            Result
	 * @param address
	 *            Address of the node that send the result
	 * @param timestamp
	 *            Timestamp of the request
	 */
	public void addResult(Result result, Address address, long timestamp) {
		if (timestamp != this.timestamp) {
			logger.warning("Old result arriving");
			return;
		}
		this.results.add(result);
		this.addresses.remove(address);
		this.semaphore.release();
		logger.info("Adding result of " + address);
	}

	/**
	 * A problem ocurred, so this request must be restarted.
	 * 
	 * @param newAddresses
	 *            New members of the request
	 * @param timestamp
	 *            New timestamp of the request
	 */
	public void restart(List<Address> newAddresses, long timestamp) {
		synchronized (this) {
			this.semaphore.drainPermits();
			this.results.clear();
			this.retry = true;
			this.addresses = newAddresses;
			this.qty.set(addresses.size() - 1);
			this.timestamp = timestamp;
		}
	}

	public Signal getSignal() {
		return signal;
	}

	public int getRequestId() {
		return requestId;
	}

	public boolean retry() {
		return this.retry;
	}

	public long getTimestamp() {
		return this.timestamp;
	}

	public List<Address> getAddresses() {
		return addresses;
	}

	public Semaphore getSemaphore() {
		return semaphore;
	}

	public List<Result> getResults() {
		return results;
	}

	public int getQty() {
		return qty.get();
	}

}
