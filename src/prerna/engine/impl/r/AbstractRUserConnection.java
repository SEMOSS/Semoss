package prerna.engine.impl.r;

import java.io.File;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.rosuda.REngine.REXP;
import org.rosuda.REngine.REXPMismatchException;
import org.rosuda.REngine.Rserve.RConnection;
import org.rosuda.REngine.Rserve.RSession;
import org.rosuda.REngine.Rserve.RserveException;

import prerna.util.Constants;
import prerna.util.Utility;

public abstract class AbstractRUserConnection implements IRUserConnection {
	
	protected static final Logger logger = LogManager.getLogger(AbstractRUserConnection.class);
	
	// keep tracked if we have stopped the R connection
	protected boolean stoppedR = false;
	
	// Recovery
	private boolean recoveryEnabled = RserveUtil.R_USER_RECOVERY_DEFAULT;
	private final String rDataFile;
	
	// R health timeout
	private static final long HEALTH_TIMEOUT = 3L;
	private static final TimeUnit HEALTH_TIMEOUT_UNIT = TimeUnit.SECONDS;
	
	// R default packages loading
	private static final long DEFAULT_PACAKGES_TIMEOUT = 20L;
	private static final TimeUnit DEFAULT_PACAKGES_UNIT = TimeUnit.SECONDS;
	
	// R timeout
	private static final long R_TIMEOUT = 7L;
	private static final TimeUnit R_TIMEOUT_UNIT = TimeUnit.HOURS;
	
	// R connection
	private Object rconMonitor = new Object();
	protected RConnection rcon;
	
	private String env;
	
	protected Process process;
	
	////////////////////////////////////////
	// Constructors, overloaded for defaults
	////////////////////////////////////////
	public AbstractRUserConnection(String rDataFile) {		
		this.rDataFile = rDataFile;
	}

	public AbstractRUserConnection() {
		this(RserveUtil.getRDataFile(Utility.getRandomString(12)));
	}
	
	////////////////////////////////////////
	// Mirroring RConnection methods
	////////////////////////////////////////
	@Override
	public REXP eval(String rScript) {
		return eval(rScript, HEALTH_TIMEOUT, HEALTH_TIMEOUT_UNIT, true);
	}
	
	protected REXP eval(String rScript, long healthTimeout, TimeUnit healthTimeoutUnit) {
		return eval(rScript, healthTimeout, healthTimeoutUnit, true);
	}
	
	private REXP eval(String rScript, long healthTimeout, TimeUnit healthTimeoutUnit, boolean retry) {
		if (isHealthy(healthTimeout, healthTimeoutUnit)) {
			if(rScript.length() > 500) {
				logger.info("Running R: " + rScript.substring(0, 500) + "...");
				logger.debug("Running R: " + rScript);
			} else {
				logger.info("Running R: " + rScript);
			}
			ExecutorService executor = Executors.newSingleThreadExecutor();
			try {
				synchronized (rconMonitor) {
					Future<REXP> future = executor.submit(new Callable<REXP>() {
						@Override
						public REXP call() throws Exception {
							REXP rexp = rcon.eval(rScript);  // fails here .. if you wrapped this.. all is well I feel.. 
							if (recoveryEnabled) {
								saveImage(); // Save image after execution
							}
							return rexp;
						}
					});
					try {
						return future.get(R_TIMEOUT, R_TIMEOUT_UNIT);
					} catch (TimeoutException | InterruptedException e) {
						logger.error(Constants.STACKTRACE, e);
						throw new IllegalArgumentException("Timout occurred when running R script.");
					} catch (ExecutionException e) {
						logger.error(Constants.STACKTRACE, e);
						throw new IllegalArgumentException("Failed to run R script.");
					}
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
					return eval(rScript, healthTimeout, healthTimeoutUnit, false);
				} else {
					throw new IllegalArgumentException("A recoverable error occurred. Please try re-running your R script.");
				}
			} else {
				throw e;
			}
		}
	}
	
	@Override
	public void voidEval(String rScript) {
		voidEval(rScript, HEALTH_TIMEOUT, HEALTH_TIMEOUT_UNIT, true);
	}
	
	protected void voidEval(String rScript, long healthTimeout, TimeUnit healthTimeoutUnit) {
		voidEval(rScript, healthTimeout, healthTimeoutUnit, true);
	}
	
	private void voidEval(String rScript, long healthTimeout, TimeUnit healthTimeoutUnit, boolean retry) {
		if (isHealthy(healthTimeout, healthTimeoutUnit)) {
			if(rScript.length() > 500) {
				logger.info("Running R: " + rScript.substring(0, 500) + "...");
				logger.debug("Running R: " + rScript);
			} else {
				logger.info("Running R: " + rScript);
			}
			ExecutorService executor = Executors.newSingleThreadExecutor();
			try {
				synchronized (rconMonitor) {
					Future<Void> future = executor.submit(new Callable<Void>() {
						@Override
						public Void call() throws Exception {
							rcon.voidEval(rScript);
							if (recoveryEnabled) {
								saveImage(); // Save image after execution
							}
							return null;
						}
					});
					try {
						future.get(R_TIMEOUT, R_TIMEOUT_UNIT);
					} catch (TimeoutException | InterruptedException e) {
						logger.error(Constants.STACKTRACE, e);
						throw new IllegalArgumentException("Timout occurred when running R script = " + rScript);
					} catch (ExecutionException e) {
						logger.error(Constants.STACKTRACE, e);
						throw new IllegalArgumentException("Failed to run R script = " + rScript);
					}
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
					voidEval(rScript, healthTimeout, healthTimeoutUnit, false);
				} else {
					throw new IllegalArgumentException("A recoverable error occurred. Please try re-running your R script.");
				}
			} else {
				throw e;
			}
		}
	}
	
	@Override
	public RSession detach() {
		if (isHealthy()) {
			logger.info("Detaching R.");
				ExecutorService executor = Executors.newSingleThreadExecutor();
				try {
					synchronized (rconMonitor) {
						Future<RSession> future = executor.submit(new Callable<RSession>() {
							@Override
							public RSession call() throws Exception {
								if (recoveryEnabled) {
									saveImage(); // Save image before detaching
								}
								return rcon.detach();
							}
						});
						try {
							return future.get(R_TIMEOUT, R_TIMEOUT_UNIT);
						} catch (TimeoutException | InterruptedException e) {
							logger.error(Constants.STACKTRACE, e);
							throw new IllegalArgumentException("Timout occurred when detaching R.");
						} catch (ExecutionException e) {
							logger.error(Constants.STACKTRACE, e);
							throw new IllegalArgumentException("Failed to detach R.");
						}
					}
				} finally {
					executor.shutdownNow();
				}
		} else {			
			throw recoveryStatus();
		}
	}
	
	
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
	// Package loading
	////////////////////////////////////////
	@Override
	public void loadDefaultPackages() {
		try {

			// load all the libraries
			ExecutorService executor = Executors.newSingleThreadExecutor();
			try {
				synchronized (rconMonitor) {
					Future<Void> future = executor.submit(new Callable<Void>() {
						@Override
						public Void call() throws Exception {

							// split stack shape
							rcon.eval("library(splitstackshape);");
							logger.info("Loaded packages splitstackshape");

							// data table
							rcon.eval("library(data.table);");
							logger.info("Loaded packages data.table");

							// reshape2
							rcon.eval("library(reshape2);");
							logger.info("Loaded packages reshape2");

							// stringr
							rcon.eval("library(stringr)");
							logger.info("Loaded packages stringr");

							// lubridate
							rcon.eval("library(lubridate);");
							logger.info("Loaded packages lubridate");

							// dplyr
							rcon.eval("library(dplyr);");
							logger.info("Loaded packages dplyr");
							return null;
						}
					});
					// sometimes this is slow on startup
					future.get(DEFAULT_PACAKGES_TIMEOUT, DEFAULT_PACAKGES_UNIT);
				}
			} finally {
				executor.shutdownNow();
			}
		} catch (Exception e) {
			logger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("Could not load R libraries.\n Please make sure the following libraries are installed:\n " +
					"1)splitstackshape\n" +
					"2)data.table\n" +
					"3)reshape2\n" +
					"4)stringr\n" +
					"5)lubridate\n" +
					"6)dplyr", e);
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
	protected boolean isHealthy(long timeout, TimeUnit timeUnit) {
		boolean beating = false; // Healthy skepticism
		
		ExecutorService executor = Executors.newSingleThreadExecutor();
		try {
			synchronized (rconMonitor) {
				Future<REXP> future = executor.submit(new Callable<REXP>() {
					@Override
					public REXP call() throws Exception {
						return rcon.eval("1+2");
					}
				});
				REXP heartBeat = future.get(timeout, timeUnit);
				if (((org.rosuda.REngine.REXP) heartBeat).asDouble() == 3L) {
					beating = true;
				}
			}
		} catch (TimeoutException | InterruptedException e) {
			logger.warn("R health check failed due to a timeout.");
			logger.error(Constants.STACKTRACE, e);
		} catch (ExecutionException e) {
			logger.warn("R health check failed");
			logger.error(Constants.STACKTRACE, e);
		} catch (REXPMismatchException e) {
			logger.warn("R health check failed due to incorrect result");
			logger.error(Constants.STACKTRACE, e);
		} finally {
			executor.shutdownNow();
		}
	
		return beating;
	}
	
	protected boolean isHealthy() {
		return isHealthy(HEALTH_TIMEOUT, HEALTH_TIMEOUT_UNIT);
	}
	
	public boolean isStopped() {
		return this.stoppedR;
	}
	
	public Process getProcess()
	{
		return process;
	}
	
}
