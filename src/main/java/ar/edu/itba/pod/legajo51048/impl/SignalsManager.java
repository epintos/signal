package ar.edu.itba.pod.legajo51048.impl;

import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import ar.edu.itba.pod.api.Signal;

public class SignalsManager {
	
	private final BlockingQueue<Signal> signals;
	private final List<Connection> connections;
	private AtomicBoolean finished = new AtomicBoolean(false);
	
	
	public SignalsManager() {
		this.signals = new LinkedBlockingQueue<Signal>();
		this.connections = new LinkedList<Connection>();
		new Thread(){
			public void run() {
				while(!finished.get()){
//					Signal signal = signals.take();
					int random =  (int) (Math.random()*(signals.size()));
//					connections.get(random).se;
				}
			};
		}.start();
	}
	
	public void addNode(Connection connection){
		this.connections.add(connection);
	}
	
	public void addSignal(Signal signal){
		this.signals.add(signal);
	}
	
	public void finish(){
		finished.set(true);
	}

}
