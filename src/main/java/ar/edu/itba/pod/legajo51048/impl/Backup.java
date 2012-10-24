package ar.edu.itba.pod.legajo51048.impl;

import org.jgroups.Address;

import ar.edu.itba.pod.api.Signal;

public class Backup {

	private Address address;
	private Signal signal;

	public Backup(Address address, Signal signal) {
		this.address = address;
		this.signal = signal;
	}
}
