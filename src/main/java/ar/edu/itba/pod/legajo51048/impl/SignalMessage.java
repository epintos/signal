package ar.edu.itba.pod.legajo51048.impl;

import org.jgroups.Address;

import ar.edu.itba.pod.api.Signal;

public class SignalMessage {
	private Signal signal;
	private boolean isBackup = false;
	private Address address;
	private boolean isBroadcastFind = false;

	public SignalMessage(Signal signal) {
		this.signal = signal;
	}

	public SignalMessage(Signal signal, boolean isBroadcastFind) {
		this.signal = signal;
		this.isBroadcastFind = true;
	}

	/**
	 * 
	 * @param address
	 *            Address from the owner of the signal
	 * @param signal
	 */
	public SignalMessage(Address address, Signal signal) {
		this.signal = signal;
		this.address = address;
		this.isBackup = true;
	}

	public Signal getSignal() {
		return signal;
	}

	public boolean isBackup() {
		return isBackup;
	}

	public Address getAddress() {
		return address;
	}
	public boolean isBroadcastFind(){
		return isBroadcastFind;
	}
}