package ar.edu.itba.pod.legajo51048.impl;

import java.io.Serializable;
import java.util.List;

import org.jgroups.Address;

import ar.edu.itba.pod.api.Signal;

public class Backup implements Serializable {

	private static final long serialVersionUID = 1L;
	
	private Address address;
	private Signal signal;
	private List<Signal> signals;

	public Backup(Address address, Signal signal) {
		this.address = address;
		this.signal = signal;
	}
	
	public Backup(Address address, List<Signal> signals) {
		this.address = address;
		this.signals = signals;
	}

	public Address getAddress() {
		return address;
	}

	public Signal getSignal() {
		return signal;
	}

	public List<Signal> getSignals() {
		return signals;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((address == null) ? 0 : address.hashCode());
		result = prime * result + ((signal == null) ? 0 : signal.hashCode());
		result = prime * result + ((signals == null) ? 0 : signals.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Backup other = (Backup) obj;
		if (address == null) {
			if (other.address != null)
				return false;
		} else if (!address.equals(other.address))
			return false;
		if (signal == null) {
			if (other.signal != null)
				return false;
		} else if (!signal.equals(other.signal))
			return false;
		if (signals == null) {
			if (other.signals != null)
				return false;
		} else if (!signals.equals(other.signals))
			return false;
		return true;
	}
	
	
	
	
}
