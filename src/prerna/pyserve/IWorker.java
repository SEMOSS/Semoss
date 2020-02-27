package prerna.pyserve;

public interface IWorker {

	// processes a given command
	void processCommand(String folder, String file);
	
	// processes a given admin command
	void processAdmin(String folder, String file);
	
	// processes the clean up
	void processCleanup(String folder, String file);
	
	// cleanup the entire dir
	void processCleanup(String folder);
	
	
}
