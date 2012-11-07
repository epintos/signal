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
	private int requestId;
	private Backup backup;
	private List<Backup> backupList;
	private long timestamp;

	/**
	 * Used for sending FIND_SIMILAR to messages
	 * 
	 * @param signal
	 *            Signal to analyze
	 * @param address
	 *            From node address
	 * @param requestId
	 *            Request id
	 * @param timestamp
	 *            Request timestamp
	 * @param type
	 *            FIND_SIMILAR_RESULT
	 */
	public SignalMessage(Signal signal, Address address, int requestId,
			long timestamp, String type) {
		this.signal = signal;
		this.type = type;
		this.address = address;
		this.requestId = requestId;
		this.timestamp = timestamp;
	}

	/**
	 * Used to distribute a result.
	 * 
	 * @param address
	 *            Principal node address
	 * @param result
	 *            Result
	 * @param id
	 *            Request id
	 * @param type
	 *            FIND_SIMILAR_RESULT
	 * @param timestamp
	 *            Timestamp of the request of this result
	 */
	public SignalMessage(Address address, Result result, int id, String type,
			long timestamp) {
		this.result = result;
		this.address = address;
		this.type = type;
		this.timestamp = timestamp;
		this.requestId = id;
	}

	/**
	 * Used for sending a signal.
	 * 
	 * @param address
	 *            From address
	 * @param signal
	 *            Signal to add
	 * @param type
	 *            ADD_SIGNAL
	 */
	public SignalMessage(Address address, Signal signal, String type) {
		this.signal = signal;
		this.address = address;
		this.type = type;
	}

	/**
	 * Used for distributing backups.
	 * 
	 * @param address
	 *            When sending backups, this address corresponds to from node
	 * @param type
	 *            ADD_BACK_UPS or ADD_BACK_UPS_ACK
	 * @param backupList
	 *            Backups sent
	 */
	public SignalMessage(Address address, String type, List<Backup> backupList) {
		this.backupList = backupList;
		this.address = address;
		this.type = type;
	}

	/**
	 * Used when a new node id added or a node falls
	 * 
	 * @param address
	 *            Address of the node fell or added
	 * @param type
	 *            BYE_NODE, NEW_NODE or IM_READY
	 */
	public SignalMessage(Address address, String type) {
		this.address = address;
		this.type = type;
	}

	/**
	 * Used for distributing a backup.
	 * 
	 * @param address
	 *            From address
	 * @param backup
	 *            Backup sent
	 * @param type
	 *            ADD_BACK_UP
	 */
	public SignalMessage(Address address, Backup backup, String type) {
		this.address = address;
		this.backup = backup;
		this.type = type;
	}

	/**
	 * Used for distributing signals or change a signal owner.
	 * 
	 * @param address
	 *            When sending signals, this address corresponds to from, else
	 *            to signal owner.
	 * @param signals
	 *            Signals distributed
	 * @param type
	 *            ADD_SIGNALS, ADD_SIGNALS_ACK or CHANGE_SIGNALS_OWNER
	 */
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

	public int getRequestId() {
		return requestId;
	}

	public Backup getBackup() {
		return backup;
	}

	public List<Backup> getBackupList() {
		return backupList;
	}

	public long getTimestamp() {
		return timestamp;
	}
}