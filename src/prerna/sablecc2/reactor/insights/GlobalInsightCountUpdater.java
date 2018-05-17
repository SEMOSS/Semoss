package prerna.sablecc2.reactor.insights;

import java.io.IOException;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import org.apache.solr.client.solrj.SolrServerException;

import prerna.solr.SolrIndexEngine;

public class GlobalInsightCountUpdater {

	/*
	 * Creating a class to manage updating the insight count
	 * This is necessary since we will get version conflicts
	 * if you run 2 insights at the same time
	 */
	
	private static GlobalInsightCountUpdater singleton;

	private BlockingQueue<String> queue;
	private CountUpdater updater;
	
	private GlobalInsightCountUpdater() {
		queue = new ArrayBlockingQueue<String>(50);
		updater = new CountUpdater(queue);
		
		new Thread(updater).start();
	}

	public static GlobalInsightCountUpdater getInstance() {
		if(singleton == null) {
			singleton = new GlobalInsightCountUpdater();
		}
		return singleton;
	}

	public void addToQueue(String engineName, String id) {
		String solrId = SolrIndexEngine.getSolrIdFromInsightEngineId(engineName, id);
		queue.add(solrId);
	}

}

class CountUpdater implements Runnable {

	protected BlockingQueue<String> queue = null;

	public CountUpdater(BlockingQueue<String> queue) {
		this.queue = queue;
	}

	public void run() {
		try {
			String solrId = null;
			while( (solrId = queue.take()) != null) {
				SolrIndexEngine.getInstance().updateViewedInsight(solrId);
			}
		} catch (KeyManagementException | NoSuchAlgorithmException | KeyStoreException | SolrServerException
				| IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
}