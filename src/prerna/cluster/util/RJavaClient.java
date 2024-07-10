//package prerna.cluster.util;
//
//import java.io.BufferedWriter;
//import java.io.File;
//import java.io.FileWriter;
//import java.io.IOException;
//
//public class RJavaClient implements Runnable{
//
//	public static void main(String [] args)
//	{
//		RJavaClient test = new RJavaClient();
//		test.registerZK();
//	}
//	
//	public String registerZK()
//	{
//		prerna.cluster.util.ZKClient client = prerna.cluster.util.ZKClient.getInstance();
//		client.publishDB(client.host);
//		
// 		return "Registered" + client.host;
// 		
// 		
//	}
//
//	@Override
//	public void run() {
//		// TODO Auto-generated method stub
//		BufferedWriter bw = null;
//		try {
//			bw = new BufferedWriter(new FileWriter(new File("c:/users/pkapaleeswaran/workspacej3/rjavaoutput.txt")));
//			while(true)
//			{
//				bw.write(".");
//				bw.flush();
//				Thread.sleep(1000);
//			}
//		} catch (IOException e) {
//			// TODO Auto-generated catch block
//			classLogger.error(Constants.STACKTRACE, e);
//		} catch (InterruptedException e) {
//			// TODO Auto-generated catch block
//			classLogger.error(Constants.STACKTRACE, e);
//		} finally {
//			if(bw!=null) {
//				try {
//					bw.close();
//				} catch (IOException e) {
//					// TODO Auto-generated catch block
//					classLogger.error(Constants.STACKTRACE, e);
//				}
//			}
//		}
//		
//	}
//	
//}
