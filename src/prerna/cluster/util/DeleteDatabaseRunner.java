package prerna.cluster.util;

public class DeleteDatabaseRunner implements Runnable {

	private final String databaseId;
	
	public DeleteDatabaseRunner(String databaseId) {
		this.databaseId = databaseId;
	}
	
	@Override
	public void run() {
		try {
			ClusterUtil.deleteDatabase(databaseId);
		} catch (Exception e) {
			e.printStackTrace();
		}		
	}

}
