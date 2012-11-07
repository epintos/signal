package ar.edu.itba.pod.legajo51048;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.rmi.AlreadyBoundException;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import ar.edu.itba.pod.legajo51048.impl.MultithreadedSignalProcessor;


public class Server {
	private final int port;
	private final int threadsQty;

	public Server(int port,int threads) {
		super();
		BasicConfigurator.configure();
        Logger.getRootLogger().setLevel(Level.INFO);
		this.port = port;
		this.threadsQty = threads;
	}

	public void start() {
		Registry reg;
		try {
			reg = LocateRegistry.createRegistry(port);
			
			MultithreadedSignalProcessor impl = new MultithreadedSignalProcessor(threadsQty);
			Remote proxy = UnicastRemoteObject.exportObject(impl, 0);

			// Since the same implementation exports both interfaces, register the same
			// proxy under the two names
			reg.bind("SignalProcessor", proxy);
			reg.bind("SPNode", proxy);
			System.out.println("Server started and listening on port " + port);
			System.out.println("Press <enter> to quit");
			new BufferedReader(new InputStreamReader(System.in)).readLine();
			
		} catch (RemoteException e) {
			System.out.println("Unable to start local server on port " + port);
			e.printStackTrace();
		} catch (IOException e) {
			System.out.println("Unexpected i/o problem");
			e.printStackTrace();
		} catch (AlreadyBoundException e) {
			System.out.println("Unable to register remote objects. Perhaps another instance is runnign on the same port?");
			e.printStackTrace();
		}
	}


	
	public static void main(String[] args) {
		if (args.length < 2) {
			System.out.println("Command line parameters: Server <port> <threads>");
			return;
		}
		new Server(Integer.parseInt(args[0]),Integer.valueOf(args[1])).start();
	}
}
