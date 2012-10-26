package ar.edu.itba.pod.legajo51048.impl;

import java.io.Serializable;
import java.util.List;

import org.jgroups.Address;

import ar.edu.itba.pod.api.Result;
import ar.edu.itba.pod.api.Signal;

public class SignalMessage implements Serializable{
	
	private static final long serialVersionUID = 1L;
	
	private Signal signal;
	private List<Signal> signals;
	private String type;
	private Address address;
	private Result result;

	public SignalMessage(Signal signal) {
		this.signal = signal;
	}

	public SignalMessage(Signal signal, String type) {
		this.signal = signal;
		this.type = type;
	}

	/**
	 * 
	 * @param address
	 *            Address from the owner of the signal
	 * @param signal
	 */
	public SignalMessage(Address address, Signal signal, String type) {
		this.signal = signal;
		this.address = address;
		this.type = type;
	}
	
	public SignalMessage(Address address, Result result, String type) {
		this.result = result;
		this.address = address;
		this.type = type;
	}

	public SignalMessage(List<Signal> signals, String type) {
		this.type = type;
		this.signals = signals;
	}
	
	public SignalMessage(Address address , List<Signal> signals, String type) {
		this.type = type;
		this.signals = signals;
		this.address = address;
	}

	public Signal getSignal() {
		return signal;
	}

	public List<Signal> getSignals() {
		return this.signals;
	}

	public Address getAddress() {
		return address;
	}

	public String getType() {
		return type;
	}
	
	public Result getResult(){
		return result;
	}
}