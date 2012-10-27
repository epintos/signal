package ar.edu.itba.pod.legajo51048.impl;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;

import ar.edu.itba.pod.api.Result;
import ar.edu.itba.pod.api.Signal;

/**
 * Worker that finds the similar signals of a signal.
 * @author Esteban G. Pintos
 *
 */
public class FindSimilarWorker implements Callable<Result> {

	private BlockingQueue<Signal> workerSignals;
	private Signal workerSignal;

	public FindSimilarWorker(Signal signal, BlockingQueue<Signal> signals) {
		this.workerSignals = signals;
		this.workerSignal = signal;
	}

	@Override
	public Result call() throws Exception {
		Result result = new Result(workerSignal);
		while (true) {
			Signal s = workerSignals.poll();
			if (s == null) {
				return result;
			}
			Result.Item item = new Result.Item(s,
					workerSignal.findDeviation(s));
			result = result.include(item);
		}
	}
}