package ar.edu.itba.pod.legajo51048.impl;

import java.io.Serializable;
import java.util.List;

import org.jgroups.Address;

import ar.edu.itba.pod.api.Result;
import ar.edu.itba.pod.api.Signal;

/**
 * Class for sending either backups, signals, ids, etc. between nodes.
 * 
 * @author Esteban G. Pintos
 * 
 */
public class SignalMessage implements Serializable {

	private static final long serialVersionUID = 123423423424L;

	private Signal signal;
	private List<Signal> signals;
	private String type;
	private Address address;
	private Result result;
	private int number;
	private Backup backup;
	private List<Backup> backupList;
	private Address otherAddress;
	private long timestamp;

	public SignalMessage(Signal signal) {
		this.signal = signal;
	}

	public SignalMessage(Signal signal, String type) {
		this.signal = signal;
		this.type = type;
	}

	public SignalMessage(Signal signal, Address address, int requestId,
			long timestamp, String type) {
		this.signal = signal;
		this.type = type;
		this.address = address;
		this.number = requestId;
		this.timestamp = timestamp;
	}

	public SignalMessage(Address address, int number, String type) {
		this.type = type;
		this.address = address;
		this.number = number;
	}

	public SignalMessage(Address address, Signal signal, String type) {
		this.signal = signal;
		this.address = address;
		this.type = type;
	}

	public SignalMessage(Address address, Address otherAddress, String type) {
		this.address = address;
		this.type = type;
		this.otherAddress = otherAddress;
	}

	public SignalMessage(Address address, String type, List<Backup> backupList) {
		this.backupList = backupList;
		this.address = address;
		this.type = type;
	}

	public SignalMessage(Address address, String type) {
		this.address = address;
		this.type = type;
	}

	public SignalMessage(Address address, Backup backup, String type) {
		this.address = address;
		this.backup = backup;
		this.type = type;
	}

	public SignalMessage(Address address, Result result, int id, String type,
			long timestamp) {
		this.result = result;
		this.address = address;
		this.type = type;
		this.timestamp = timestamp;
		this.number = id;
	}

	public SignalMessage(Address address, List<Signal> signals, String type) {
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

	public Result getResult() {
		return result;
	}

	public int getNumber() {
		return number;
	}

	public Backup getBackup() {
		return backup;
	}

	public List<Backup> getBackupList() {
		return backupList;
	}

	public Address getOtherAddress() {
		return otherAddress;
	}

	public long getTimestamp() {
		return timestamp;
	}
}