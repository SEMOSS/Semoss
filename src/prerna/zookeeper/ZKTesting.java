package prerna.zookeeper;

import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.curator.framework.recipes.locks.InterProcessSemaphoreV2;
import org.apache.curator.framework.recipes.locks.Lease;

import com.google.gson.Gson;

public class ZKTesting {

	public static void main(String[] args) throws Exception {
		ZKEngine engine = new ZKEngine();
		try {
			Properties prop = new Properties();
			prop.put(ZKEngine.ZOOKEEPER_ADDRESS_KEY, "localhost:2181");
			engine.open(prop);

			ZKCuratorUtility utility = engine.getCuratorUtility();
			startTimeLockThread(utility);
			Thread.sleep(3_000);
			printTimeLockDetails(utility);
		} finally {
			engine.close();
		}
	}

	public static void startTimeLockThread(ZKCuratorUtility utility) {
		Thread t = new Thread(new Runnable() {

			@Override
			public void run() {
				InterProcessSemaphoreV2 timebasedLock = null;
				Lease lease = null;
				try {
					timebasedLock = utility.getTimeBasedLock("/testLock", 1);
					lease = timebasedLock.acquire(10, TimeUnit.MINUTES);
					
					LocalDateTime startTime = LocalDateTime.now();
					
					Map<String, Object> dataMap = new HashMap<>();
					dataMap.put("startTime", startTime);
					dataMap.put("endTime", startTime.plusMinutes(10));
					dataMap.put("lockTime", 10);
					dataMap.put("lockUnit", TimeUnit.MINUTES);
					dataMap.put("user", "maher.khalil@va.gov");
					Gson gson = new Gson();
					byte[] data = gson.toJson(dataMap).getBytes();
					utility.createEphemerialNode("/userWithLock", data);
				} catch(Exception e) {
					e.printStackTrace();
				} finally {
					if(timebasedLock != null && lease != null) {
						timebasedLock.returnLease(lease);
					}
				}
			}
		});
		t.start();
	}

	public static void printTimeLockDetails(ZKCuratorUtility utility) throws Exception {
		byte[] data = utility.getDataFromNode("/userWithLock");
		String dataStr = new String(data);
		Map<String, Object> dataMap = new Gson().fromJson(dataStr, Map.class);
		System.out.println(dataMap);
		
		
		LocalDateTime currentTime = LocalDateTime.now();
		LocalDateTime endTime = new Gson().fromJson(dataMap.get("endTime").toString(), LocalDateTime.class);

		System.out.println("Time left for lock = " + currentTime.until(endTime, ChronoUnit.MINUTES));
		System.out.println("Time left for lock = " + currentTime.until(endTime, ChronoUnit.MINUTES));
		System.out.println("Time left for lock = " + currentTime.until(endTime, ChronoUnit.MINUTES));
		System.out.println("Time left for lock = " + currentTime.until(endTime, ChronoUnit.MINUTES));
		System.out.println("Time left for lock = " + currentTime.until(endTime, ChronoUnit.MINUTES));
		System.out.println("Time left for lock = " + currentTime.until(endTime, ChronoUnit.MINUTES));
		System.out.println("Time left for lock = " + currentTime.until(endTime, ChronoUnit.MINUTES));
	}

}
