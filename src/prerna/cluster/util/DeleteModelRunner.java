//package prerna.cluster.util;
//
//public class DeleteModelRunner implements Runnable {
//
//	private final String modelId;
//	
//	public DeleteModelRunner(String modelId) {
//		this.modelId = modelId;
//	}
//	
//	@Override
//	public void run() {
//		try {
//			ClusterUtil.deleteStorage(modelId);
//		} catch (Exception e) {
//			e.printStackTrace();
//		}		
//	}
//
//}
