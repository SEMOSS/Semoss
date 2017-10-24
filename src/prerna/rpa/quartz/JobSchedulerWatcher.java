package prerna.rpa.quartz;

import java.io.File;

import prerna.rpa.RPAProps;
import prerna.rpa.config.JobConfigParser;
import prerna.util.AbstractFileWatcher;

public class JobSchedulerWatcher extends AbstractFileWatcher {

	@Override
	public void loadFirst() {
		File dir = new File(folderToWatch);
		String[] fileNames = dir.list();
		if (fileNames != null)
			for (String fName : fileNames) {
				RPAProps.getInstance();
				process(fName);
			}
	}

	@Override
	public void process(String fileName) {
		try {
			if (fileName.endsWith(".json")) {
				JobConfigParser.parse(fileName, false);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public void run() {
		logger.info("Starting JobSchedulerWatcher thread");
		loadFirst();
		super.run();
	}
}
