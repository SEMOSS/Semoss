package prerna.engine.impl.r;

import java.io.File;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang.SystemUtils;
import org.rosuda.REngine.Rserve.RConnection;

import prerna.util.DIHelper;

public class RUserRserve {
	
	private static final String FS = System.getProperty("file.separator");
	
	private static final int PORT = 6311;
	private static final String HOST = "127.0.0.1";
	private static final String R_HOME_KEY = "R_HOME";
	
	private static String rBin;
	
	// Get the R binary location
	static {
		String rHome = System.getenv(R_HOME_KEY).replace("\\", FS);
		if (rHome == null || rHome.isEmpty()) {
			rBin = "R"; // Just hope its in the path
		} else {
			rBin = rHome + FS + "bin" + FS + "R";
			if (SystemUtils.IS_OS_WINDOWS) rBin = rBin.replace(FS, "\\\\");
		}
	}
	
	private static void startRserve() throws Exception {
		String baseFolder = DIHelper.getInstance().getProperty("BaseFolder");

		File output = new File(baseFolder + FS + "Rserve.output.log");
		File error = new File(baseFolder + FS + "Rserve.error.log");

		ProcessBuilder pb;
		if (SystemUtils.IS_OS_WINDOWS) {
			pb = new ProcessBuilder(rBin, "-e", "library(Rserve);Rserve(FALSE," + PORT
					+ ",args='--vanilla');flush.console <- function(...) {return;};options(error=function() NULL)",
					"--vanilla");
		} else {
			pb = new ProcessBuilder(rBin, "CMD", "Rserve", "--vanilla", "--RS-port", PORT + "");
		}
		pb.redirectOutput(output);
		pb.redirectError(error);
		Process process = pb.start();
		process.waitFor(7L, TimeUnit.SECONDS);
	}

	public static void stopRserve() throws Exception {
		ProcessBuilder pb;
		if (SystemUtils.IS_OS_WINDOWS) {
			pb = new ProcessBuilder("taskkill", "/f", "/IM", "Rserve.exe");
		} else {
			pb = new ProcessBuilder("pkill", "Rserve");
		}
		Process process = pb.start();
		process.waitFor(7L, TimeUnit.SECONDS);
	}
		
	private static RConnection getConnection() throws Exception {
		ExecutorService executor = Executors.newSingleThreadExecutor();
		try {
			Future<RConnection> future = executor.submit(new Callable<RConnection>() {
				@Override
				public RConnection call() throws Exception {
					return new RConnection(HOST, PORT);
				}
			});
			return future.get(7L, TimeUnit.SECONDS); 
		} finally {
			executor.shutdownNow();
		}
	}
		
	public synchronized static RConnection createConnection() {
		RConnection rcon;
		try {
			
			// Try establishing a connection to an existing Rserve
			rcon = getConnection();
		} catch (Exception e0) {
			try {
				
				// If that doesn't work, try starting Rserve
				startRserve();
				rcon = getConnection();
			} catch (Exception e1) {
				try {
					
					// If that doesn't work, try restarting Rserve
					stopRserve();
					startRserve();
					rcon = getConnection();
				} catch (Exception e3) {
					throw new IllegalArgumentException("Unable to establish R connection.", e3);
				}
			}
		}
		return rcon;
	}

}