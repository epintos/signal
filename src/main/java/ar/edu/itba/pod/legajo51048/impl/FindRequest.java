package ar.edu.itba.pod.legajo51048.impl;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Semaphore;

import org.jgroups.Address;

import ar.edu.itba.pod.api.Result;

public class FindRequest {

	private int id;
	private List<Address> addresses;
	private int qty;
	private Semaphore semaphore;
	private List<Result> results;

	public FindRequest(int id, List<Address> addresses, int qty,Semaphore semaphore) {
		this.id = id;
		this.addresses = addresses;
		this.qty = qty;
		this.semaphore = semaphore;
		this.results = new ArrayList<Result>();
	}

	public int getId() {
		return id;
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
		return results;
	}

	public void removeAddress(Address address){
		this.results.remove(address);
	}
	public void addResult(Result result){
		this.results.add(result);
	}
	
}
