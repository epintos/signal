package ar.edu.itba.pod.legajo51048.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Semaphore;

import org.jgroups.Address;

import ar.edu.itba.pod.api.Result;

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
	private int qty;

	private Semaphore semaphore;

	// Results of the request
	private List<Result> results;

	private boolean finishedOk = true;

	private List<Address> finishedDistributing = new ArrayList<Address>();

	public FindRequest(List<Address> addresses, int qty, Semaphore semaphore) {
		this.addresses = addresses;
		this.qty = qty;
		this.semaphore = semaphore;
		this.results = new ArrayList<Result>();
	}

	public List<Address> getAddresses() {
		return addresses;
	}

	public Semaphore getSemaphore() {
		return semaphore;
	}

	public int getQty() {
		return qty;
	}

	public List<Result> getResults() {
		if (qty != 0) {
			System.out.println("no deberia pasar, qty!=0");
		}
		return results;
	}

	public void removeAddress(Address address) {
		this.results.remove(address);
		qty--;
	}

	public void addResult(Result result) {
		this.results.add(result);
	}

	public boolean finishedOk() {
		return finishedOk;
	}

	public void abort() {
		semaphore.release(qty);
		finishedOk = false;
	}

	public boolean finishedDistributing(int qty) {
		return finishedDistributing.size() == qty;
	}
	
	public void restart(){
		
	}

}
