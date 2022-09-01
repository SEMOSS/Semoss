//package prerna.test;
//
//public class ThreadLocalTestRunnable implements Runnable {
//
//	String t;
//	
//	public ThreadLocalTestRunnable(String t)
//	{
//		this.t = t;
//	}
//	
//	@Override
//	public void run() {
//		// TODO Auto-generated method stub
//		try {
//			System.out.println("Setting t to " + t);
//			UserStore.getThreadLocal().set("Hello " + t);
//			System.out.println("Going to sleep.. ");
//			Thread.sleep(3000);
//			System.out.println(">> " + UserStore.getThreadLocal().get());
//		} catch (InterruptedException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
//
//	}
//
//}
