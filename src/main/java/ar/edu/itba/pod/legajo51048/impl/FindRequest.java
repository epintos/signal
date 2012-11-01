package ar.edu.itba.pod.legajo51048.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;

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

	// Quantity of nodes in the request
	private AtomicInteger qty;

	private Semaphore semaphore;

	// Results of the request
	private List<Result> results;

	private Signal signal;
	private int requestId;

	public FindRequest(int requestId, Signal signal, List<Address> addresses,
			Semaphore semaphore) {
		this.addresses = addresses;
		
		// Remove the principal node
		this.qty = new AtomicInteger(addresses.size() - 1);
		this.requestId = requestId;
		this.signal = signal;
		this.semaphore = semaphore;
		this.results = new ArrayList<Result>();
	}

	public List<Address> getAddresses() {
		return addresses;
	}

	public Semaphore getSemaphore() {
		return semaphore;
	}

	public List<Result> getResults() {
		if (qty.get() != 0) {
			System.out.println("no deberia pasar, qty!=0");
		}
		return results;
	}

	public int getQty() {
		return qty.get();
	}

	public void addResult(Result result, Address address) {
		this.results.add(result);
		this.addresses.remove(address);
		this.qty.decrementAndGet();
		this.semaphore.release();
		System.out.println("agrego resultado de " + address);
	}

	public void restart(List<Address> newAddresses) {
		this.semaphore.drainPermits();
		this.results.clear();
		this.addresses = newAddresses;
		this.qty.set(addresses.size() - 1);
	}

	public Signal getSignal() {
		return signal;
	}

	public int getRequestId() {
		return requestId;
	}

}
