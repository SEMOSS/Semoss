package prerna.sablecc2.reactor.insights;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import prerna.auth.utils.SecurityUpdateUtils;

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
		queue = new ArrayBlockingQueue<String[]>(50);
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
		queue.add(new String[]{engineId, id});
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
				SecurityUpdateUtils.updateExecutionCount(update[0], update[1]);
			}
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
}