package prerna.reactor.insights;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import prerna.auth.utils.SecurityInsightUtils;

public class GlobalInsightCountUpdater {

	/*
	 * Creating a class to manage updating the insight count
	 * This is necessary since we will get version conflicts
	 * if you run 2 insights at the same time
	 */

	private static GlobalInsightCountUpdater singleton;

	private BlockingQueue<String[]> queue;
	private CountUpdater updater;

	private GlobalInsightCountUpdater() {
		queue = new ArrayBlockingQueue<String[]>(2_000);
		updater = new CountUpdater(queue);

		new Thread(updater).start();
	}

	public static GlobalInsightCountUpdater getInstance() {
		if(singleton == null) {
			singleton = new GlobalInsightCountUpdater();
		}
		return singleton;
	}

	public void addToQueue(String engineId, String id) {
		try {
			queue.add(new String[]{engineId, id});
		} catch(Exception e) {
			e.printStackTrace();
		}
	}

}

class CountUpdater implements Runnable {

	protected BlockingQueue<String[]> queue = null;

	public CountUpdater(BlockingQueue<String[]> queue) {
		this.queue = queue;
	}

	public void run() {
		try {
			String[] update = null;
			while( (update = queue.take()) != null) {
				SecurityInsightUtils.updateExecutionCount(update[0], update[1]);
			}
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
}