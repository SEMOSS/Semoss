package prerna.engine.impl.r;

import java.io.File;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.rosuda.REngine.REXP;
import org.rosuda.REngine.Rserve.RConnection;
import org.rosuda.REngine.Rserve.RSession;
import org.rosuda.REngine.Rserve.RserveException;

import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

public abstract class AbstractRUserConnection implements IRUserConnection {
	
	protected static final Logger LOGGER = LogManager.getLogger(AbstractRUserConnection.class.getName());
	
	// File structure
	private static final String R_FOLDER = (DIHelper.getInstance().getProperty(Constants.BASE_FOLDER) + "/" + "R" + "/" + "Temp" + "/").replace('\\', '/');
	
	// Recovery
	private boolean recoveryEnabled = false;
	private final String rDataFile;
	private static final String R_DATA_EXT = ".RData";
	
	// R timeout
	private static final long R_TIMEOUT = 7L;
	private static final TimeUnit R_TIMEOUT_UNIT = TimeUnit.HOURS;
	
	// R connection
	private Object rconMonitor = new Object();
	private RConnection rcon;
	
	
	////////////////////////////////////////
	// Constructors, overloaded for defaults
	////////////////////////////////////////
	public AbstractRUserConnection(String rDataFileName) {
		this.rDataFile = R_FOLDER + rDataFileName + R_DATA_EXT;
	}

	public AbstractRUserConnection() {
		this(Utility.getRandomString(12));
	}
	
	
	////////////////////////////////////////
	// Used to establish the rcon
	////////////////////////////////////////
	@Override
	public void initializeConnection() throws Exception {
		ExecutorService executor = Executors.newSingleThreadExecutor();
		try {
			Future<RConnection> future = executor.submit(new Callable<RConnection>() {
				@Override
				public RConnection call() throws Exception {
					return new RConnection(getHost(), getPort());
				}
			});
			rcon = future.get(7L, TimeUnit.SECONDS); 
		} finally {
			executor.shutdownNow();
		}
	}
	
	protected abstract String getHost();
	
	protected abstract int getPort();
	
	
	////////////////////////////////////////
	// Raw R connection
	////////////////////////////////////////
	// TODO >>>timb: R - should get rid of this (later)
	@Deprecated
	@Override
	public RConnection getRConnection() {
		return rcon;
	}
	
	
	////////////////////////////////////////
	// Mirroring RConnection methods
	////////////////////////////////////////
	@Override
	public REXP eval(String rScript) {
		return eval(rScript, true);
	}
	
	private REXP eval(String rScript, boolean retry) {
		if (isHealthy()) {
			LOGGER.info("Running R: " + rScript);
			ExecutorService executor = Executors.newSingleThreadExecutor();
			try {
				Future<REXP> future = executor.submit(new Callable<REXP>() {
					@Override
					public REXP call() throws Exception {
						if (recoveryEnabled) saveImage();
						synchronized (rconMonitor) {
							return rcon.eval(rScript);
						}
					}
				});
				try {
					return future.get(R_TIMEOUT, R_TIMEOUT_UNIT);
				} catch (TimeoutException | InterruptedException e) {
					throw new IllegalArgumentException("Timout occured when running R script.", e);
				} catch (ExecutionException e) {
					throw new IllegalArgumentException("Failed to run R script.", e);
				}
			} finally {
				executor.shutdownNow();
			}
		} else {
			
			// If there was no exception with the recovery, then retry once more
			// Otherwise, throw the exception
			IllegalArgumentException e = recoveryStatus();
			if (e == null) {
				if (retry) {
					return eval(rScript, false);
				} else {
					throw new IllegalArgumentException("A recoverable error occured. Please try re-running your R script.");
				}
			} else {
				throw e;
			}
		}
	}
	
	@Override
	public void voidEval(String rScript) {
		voidEval(rScript, true);
	}
	
	private void voidEval(String rScript, boolean retry) {
		if (isHealthy()) {
			LOGGER.info("Running R: " + rScript);
				ExecutorService executor = Executors.newSingleThreadExecutor();
				try {
					Future<Void> future = executor.submit(new Callable<Void>() {
						@Override
						public Void call() throws Exception {
							if (recoveryEnabled) saveImage();
							synchronized (rconMonitor) {
								rcon.voidEval(rScript);
							}
							return null;
						}
					});
					try {
						future.get(R_TIMEOUT, R_TIMEOUT_UNIT);
					} catch (TimeoutException | InterruptedException e) {
						throw new IllegalArgumentException("Timout occured when running R script.", e);
					} catch (ExecutionException e) {
						throw new IllegalArgumentException("Failed to run R script.", e);
					}
				} finally {
					executor.shutdownNow();
				}
		} else {
			
			// If there was no exception with the recovery, then retry once more
			// Otherwise, throw the exception
			IllegalArgumentException e = recoveryStatus();
			if (e == null) {
				if (retry) {
					voidEval(rScript, false);
				} else {
					throw new IllegalArgumentException("A recoverable error occured. Please try re-running your R script.");
				}
			} else {
				throw e;
			}
		}
	}
	
	@Override
	public RSession detach() {
		if (isHealthy()) {
			LOGGER.info("Detaching R.");
				ExecutorService executor = Executors.newSingleThreadExecutor();
				try {
					Future<RSession> future = executor.submit(new Callable<RSession>() {
						@Override
						public RSession call() throws Exception {
							if (recoveryEnabled) saveImage();
							synchronized (rconMonitor) {
								return rcon.detach();
							}
						}
					});
					try {
						return future.get(R_TIMEOUT, R_TIMEOUT_UNIT);
					} catch (TimeoutException | InterruptedException e) {
						throw new IllegalArgumentException("Timout occured when detaching R.", e);
					} catch (ExecutionException e) {
						throw new IllegalArgumentException("Failed to detach R.", e);
					}
				} finally {
					executor.shutdownNow();
				}
		} else {			
			throw recoveryStatus();
		}
	}
	
	
	////////////////////////////////////////
	// Stopping
	////////////////////////////////////////
	@Override
	public abstract void stopR() throws Exception;
	
	
	////////////////////////////////////////
	// Cancellation
	////////////////////////////////////////
	@Override
	public abstract void cancelExecution() throws Exception;
	
	
	////////////////////////////////////////
	// Recovery
	////////////////////////////////////////
	private IllegalArgumentException recoveryStatus() {
		IllegalArgumentException exception;
		String message = "Failed to connect to R. ";
		
		// Try and recover
		try {			
			recoverConnection();
			
			// If recovery is enabled, also try to reload the R data
			message += "The connection has recovered; however, your R data has been lost.";
			if (recoveryEnabled) {
				try {
					loadImage();
					exception = null;
				} catch (RserveException e) {
					exception = new IllegalArgumentException(message, e);
				}
			} else {
				exception = new IllegalArgumentException(message);
			}
		} catch (Exception e) {
			exception = new IllegalArgumentException(message, e);
		}
		
		return exception;
	}
	
	protected abstract void recoverConnection() throws Exception;
	
	private void saveImage() throws RserveException {
		if (rDataFile == null) throw new IllegalArgumentException("Cannot save workspace image, as the RData file location is not defined.");
		if (!new File(rDataFile).getParentFile().exists()) throw new IllegalArgumentException("Cannot save workspace image, as the RData file folder is not defined.");
		synchronized (rconMonitor) {
			rcon.voidEval("save.image(file = \""+ rDataFile + "\")");
		}
	}
	
	private void loadImage() throws RserveException {
		if (rDataFile == null) throw new IllegalArgumentException("Cannot load workspace image, as the RData file location is not defined.");
		if (!new File(rDataFile).exists()) throw new IllegalArgumentException("Cannot load workspace image, as the RData file is not defined.");
		synchronized (rconMonitor) {
			rcon.voidEval("load(\"" + rDataFile + "\")");
		}
	}

	@Override
	public boolean isRecoveryEnabled() {
		return recoveryEnabled;
	}

	@Override
	public void setRecoveryEnabled(boolean enableRecovery) {
		this.recoveryEnabled = enableRecovery;
	}
		
	
	////////////////////////////////////////
	// Health check
	////////////////////////////////////////
	protected boolean isHealthy() {
		boolean beating = false; // Healthy skepticism
		
		ExecutorService executor = Executors.newSingleThreadExecutor();
		Future<REXP> future = executor.submit(new Callable<REXP>() {
			@Override
			public REXP call() throws Exception {
				synchronized (rconMonitor) {
					return rcon.eval("1+2");
				}
			}
		});

		try {
			REXP heartBeat = future.get(3L, TimeUnit.SECONDS);
			if (((org.rosuda.REngine.REXP) heartBeat).asDouble() == 3L) {
				beating = true;
			}
		} catch (Exception e) {
			LOGGER.warn("R health check failed.");
		} finally {
			executor.shutdownNow();
		}
	
		return beating;
	}
	
	
}
