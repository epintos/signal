package ar.edu.itba.pod.legajo51048.impl;

public interface SignalMessageType {

	public static final String BACK_UP = "BACK_UP";
	public static final String BACK_UPS = "BACK_UPS";
	public static final String BYE_NODE = "BYE_NODE";
	public static final String YOUR_SIGNAL = "YOUR_SIGNAL";
	public static final String YOUR_SIGNALS = "YOUR_SIGNALS";
	public static final String CHANGE_BACK_UP_OWNER = "CHANGE_BACK_UP_OWNER";
	public static final String FIND_SIMILAR = "FIND_SIMILAR";
	public static final String ASKED_RESULT = "ASKED_RESULT";
	public static final String BACKUP_REDISTRIBUTION = "BACKUP_REDISTRIBUTION";
	public static final String ADD_BACKUP_OWNER = "ADD_BACKUP_OWNER";

	/** For node notifications **/
	public static final String REQUEST_NOTIFICATION = "REQUEST_NOTIFICATION";
	public static final String ADD_SIGNAL_ACK = "ADD_SIGNAL_ACK";
	public static final String ADD_SIGNAL_NACK = "ADD_SIGNAL_NACK";
	public static final String ADD_SIGNALS_ACK = "ADD_SIGNALS_ACK";
	public static final String ADD_SIGNALS_NACK = "ADD_SIGNALS_NACK";
	public static final String ADD_BACKUP_ACK = "ADD_BACKUP_ACK";
	public static final String ADD_BACKUP_NACK = "ADD_BACKUP_NACK";
	public static final String BACKUP_REDISTRIBUTION_ACK = "BACKUP_REDISTRIBUTION_ACK";
	public static final String BACKUP_REDISTRIBUTION_NACK = "BACKUP_REDISTRIBUTION_NACK";
	public static final String NEW_NODE = "NEW_NODE";

}
