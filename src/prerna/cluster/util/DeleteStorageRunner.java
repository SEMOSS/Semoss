//package prerna.cluster.util;
//
//public class DeleteStorageRunner implements Runnable {
//
//	private final String storageId;
//	
//	public DeleteStorageRunner(String storageId) {
//		this.storageId = storageId;
//	}
//	
//	@Override
//	public void run() {
//		try {
//			ClusterUtil.deleteStorage(storageId);
//		} catch (Exception e) {
//			e.printStackTrace();
//		}		
//	}
//
//}
