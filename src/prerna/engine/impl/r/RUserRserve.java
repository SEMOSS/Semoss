package prerna.engine.impl.r;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang.SystemUtils;
import org.rosuda.REngine.Rserve.RConnection;
import org.rosuda.REngine.Rserve.RserveException;

import prerna.util.DIHelper;

public class RUserRserve {
	
	private static final String FS = System.getProperty("file.separator");
	
	private static final int PORT = 6311;
	private static final String HOST = "127.0.0.1";
	private static final String R_HOME_KEY = "R_HOME";
	
	private static volatile boolean started = false;
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
	
	private static void startRServe() throws Exception {
		String baseFolder = DIHelper.getInstance().getProperty("BaseFolder");

		File output = new File(baseFolder + FS + "Rserve.output.log");
		File error = new File(baseFolder + FS + "Rserve.error.log");
		
		ProcessBuilder pb;
		if (SystemUtils.IS_OS_WINDOWS) {
			pb = new ProcessBuilder(rBin, "-e", "library(Rserve);Rserve(FALSE," + PORT + ",args='--vanilla');flush.console <- function(...) {return;};options(error=function() NULL)", "--vanilla");
		} else {
			pb = new ProcessBuilder(rBin, "CMD", "Rserve", "--vanilla", "--RS-port", PORT + "");
		}
		pb.redirectOutput(output);
		pb.redirectError(error);
		Process process = pb.start();
		process.waitFor(7, TimeUnit.SECONDS);
		started = true;
	}

	public static void stopRServe() throws Exception {
		ProcessBuilder pb;
		if (SystemUtils.IS_OS_WINDOWS) {
			pb = new ProcessBuilder(rBin, "-e", "library(Rserve);library(RSclient);rsc<-RSconnect(port=" + PORT + ");RSshutdown(rsc)", "--vanilla");
		} else {
			pb = new ProcessBuilder("pkill", "Rserve");
		}
		Process process = pb.start();
		process.waitFor(7, TimeUnit.SECONDS);
		started = false;
	}
	
	public synchronized static RConnection createConnection() throws Exception {
		RConnection rcon;
		try {
			rcon = new RConnection(HOST, PORT);
		} catch (RserveException e0) {
						
			// try to start again and see if that works
			try {
				if (!started) {
					try {
						startRServe();
					} catch (Exception e) {
						
						// If some sort of issue occurs, then try stopping then starting
						stopRServe();
						startRServe();
					}
				} else {
					stopRServe();
					startRServe();
				}
				rcon = new RConnection(HOST, PORT);
			} catch (RserveException | IOException e1) {
				throw new IllegalArgumentException("Unable to establish R connection.", e1);
			}
		}
		return rcon;
	}

}