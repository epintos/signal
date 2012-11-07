package ar.edu.itba.pod.legajo51048.messages;

import java.io.Serializable;

import org.jgroups.Address;

import ar.edu.itba.pod.api.Signal;

/**
 * Class that contains a signal and the address of the owner.
 * 
 * @author Esteban G. Pintos
 * 
 */
public class Backup implements Serializable {

	private static final long serialVersionUID = 24524521L;

	private Address address;
	private Signal signal;

	public Backup(Address address, Signal signal) {
		this.address = address;
		this.signal = signal;
	}

	public Address getAddress() {
		return address;
	}

	public Signal getSignal() {
		return signal;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((address == null) ? 0 : address.hashCode());
		result = prime * result + ((signal == null) ? 0 : signal.hashCode());
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
		return true;
	}

}
