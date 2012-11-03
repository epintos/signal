package ar.edu.itba.pod.legajo51048.impl;

/**
 * Interface containing all kind of messages that can be sent between nodes.
 * 
 * @author Esteban G. Pintos
 * 
 */
public interface SignalMessageType {

	public static final String ADD_BACK_UP = "ADD_BACK_UP";
	public static final String ADD_BACK_UPS = "ADD_BACK_UPS";
	public static final String BYE_NODE = "BYE_NODE";
	public static final String ADD_SIGNAL = "ADD_SIGNAL";
	public static final String ADD_SIGNALS = "ADD_SIGNALS";
	public static final String CHANGE_SIGNALS_OWNER = "CHANGE_SIGNALS_OWNER";
	public static final String FIND_SIMILAR = "FIND_SIMILAR";
	public static final String ASKED_RESULT = "ASKED_RESULT";
	public static final String GENERATE_NEW_SIGNALS_FROM_BACKUP = "GENERATE_NEW_SIGNALS_FROM_BACKUP";
	public static final String IM_READY = "IM_READY";
	public static final String FINISHED_REDISTRIBUTION = "FINISHED_REDISTRIBUTION";
	public static final String READY_FOR_REDISTRIBUTION = "READY_FOR_REDISTRIBUTION";

	public static final String REQUEST_NOTIFICATION = "REQUEST_NOTIFICATION";
	public static final String ADD_SIGNAL_ACK = "ADD_SIGNAL_ACK";
	public static final String ADD_SIGNALS_ACK = "ADD_SIGNALS_ACK";
	public static final String ADD_BACKUP_ACK = "ADD_BACKUP_ACK";
	public static final String ADD_BACKUPS_ACK = "ADD_BACKUPS_ACK";
	public static final String GENERATE_NEW_SIGNALS_FROM_BACKUP_ACK = "GENERATE_NEW_SIGNALS_FROM_BACKUP_ACK";
	public static final String NEW_NODE = "NEW_NODE";

}
