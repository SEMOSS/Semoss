/*******************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
 *
 * If your use of this software does not include any GPLv2 components:
 * 	Licensed under the Apache License, Version 2.0 (the "License");
 * 	you may not use this file except in compliance with the License.
 * 	You may obtain a copy of the License at
 *
 * 	  http://www.apache.org/licenses/LICENSE-2.0
 *
 * 	Unless required by applicable law or agreed to in writing, software
 * 	distributed under the License is distributed on an "AS IS" BASIS,
 * 	WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * 	See the License for the specific language governing permissions and
 * 	limitations under the License.
 * ----------------------------------------------------------------------------
 * If your use of this software includes any GPLv2 components:
 * 	This program is free software; you can redistribute it and/or
 * 	modify it under the terms of the GNU General Public License
 * 	as published by the Free Software Foundation; either version 2
 * 	of the License, or (at your option) any later version.
 *
 * 	This program is distributed in the hope that it will be useful,
 * 	but WITHOUT ANY WARRANTY; without even the implied warranty of
 * 	MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * 	GNU General Public License for more details.
 *******************************************************************************/
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

import prerna.util.Utility;

public abstract class AbstractRUserConnection implements IRUserConnection {

	protected static final Logger classLogger = LogManager.getLogger(AbstractRUserConnection.class);

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
			if (rScript.length() > 500) {
				classLogger.info("Running R script (truncated): {}...", rScript.substring(0, 500));
				classLogger.debug("Running R script: {}", rScript);
			} else {
				classLogger.info("Running R script: {}", rScript);
			}
			try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {
				synchronized (rconMonitor) {
					Future<REXP> future = executor.submit(new Callable<REXP>() {
						@Override
						public REXP call() throws Exception {
							REXP rexp = rcon.eval(rScript); // fails here .. if you wrapped this.. all is well I
															// feel..
							if (recoveryEnabled) {
								saveImage(); // Save image after execution
							}
							return rexp;
						}
					});
					try {
						return future.get(R_TIMEOUT, R_TIMEOUT_UNIT);
					} catch (TimeoutException | InterruptedException e) {
						classLogger.error("Timed out or interrupted waiting for R eval to complete", e);
						throw new IllegalArgumentException("Timout occurred when running R script.");
					} catch (ExecutionException e) {
						classLogger.error("R eval threw an exception during execution", e);
						throw new IllegalArgumentException("Failed to run R script.");
					}
				}
			}
		} else {

			// If there was no exception with the recovery, then retry once more
			// Otherwise, throw the exception
			IllegalArgumentException e = recoveryStatus();
			if (e == null) {
				if (retry) {
					return eval(rScript, healthTimeout, healthTimeoutUnit, false);
				} else {
					throw new IllegalArgumentException(
							"A recoverable error occurred. Please try re-running your R script.");
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
			if (rScript.length() > 500) {
				classLogger.info("Running R script (truncated): {}...", rScript.substring(0, 500));
				classLogger.debug("Running R script: {}", rScript);
			} else {
				classLogger.info("Running R script: {}", rScript);
			}
			try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {
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
						classLogger.error("Timed out or interrupted waiting for R voidEval to complete", e);
						throw new IllegalArgumentException("Timout occurred when running R script = " + rScript);
					} catch (ExecutionException e) {
						classLogger.error("R voidEval threw an exception during execution", e);
						throw new IllegalArgumentException("Failed to run R script = " + rScript);
					}
				}
			}
		} else {

			// If there was no exception with the recovery, then retry once more
			// Otherwise, throw the exception
			IllegalArgumentException e = recoveryStatus();
			if (e == null) {
				if (retry) {
					voidEval(rScript, healthTimeout, healthTimeoutUnit, false);
				} else {
					throw new IllegalArgumentException(
							"A recoverable error occurred. Please try re-running your R script.");
				}
			} else {
				throw e;
			}
		}
	}

	@Override
	public RSession detach() {
		if (isHealthy()) {
			classLogger.info("Detaching R.");
			try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {
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
						classLogger.error("Timed out or interrupted waiting for R session to detach", e);
						throw new IllegalArgumentException("Timout occurred when detaching R.");
					} catch (ExecutionException e) {
						classLogger.error("R detach threw an exception during execution", e);
						throw new IllegalArgumentException("Failed to detach R.");
					}
				}
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
			try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {
				synchronized (rconMonitor) {
					Future<Void> future = executor.submit(new Callable<Void>() {
						@Override
						public Void call() throws Exception {

							// split stack shape
							rcon.eval("library(splitstackshape);");
							classLogger.info("Loaded packages splitstackshape");

							// data table
							rcon.eval("library(data.table);");
							classLogger.info("Loaded packages data.table");

							// reshape2
							rcon.eval("library(reshape2);");
							classLogger.info("Loaded packages reshape2");

							// stringr
							rcon.eval("library(stringr)");
							classLogger.info("Loaded packages stringr");

							// lubridate
							rcon.eval("library(lubridate);");
							classLogger.info("Loaded packages lubridate");

							// dplyr
							rcon.eval("library(dplyr);");
							classLogger.info("Loaded packages dplyr");
							return null;
						}
					});
					// sometimes this is slow on startup
					future.get(DEFAULT_PACAKGES_TIMEOUT, DEFAULT_PACAKGES_UNIT);
				}
			}
		} catch (Exception e) {
			classLogger.error(
					"Failed to load one or more default R libraries (splitstackshape, data.table, reshape2, stringr, lubridate, dplyr)",
					e);
			throw new IllegalArgumentException(
					"Could not load R libraries.\n Please make sure the following libraries are installed:\n "
							+ "1)splitstackshape\n" + "2)data.table\n" + "3)reshape2\n" + "4)stringr\n"
							+ "5)lubridate\n" + "6)dplyr",
					e);
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
		if (rDataFile == null) {
			throw new IllegalArgumentException(
					"Cannot save workspace image, as the RData file location is not defined.");
		}
		if (!new File(rDataFile).getParentFile().exists()) {
			throw new IllegalArgumentException("Cannot save workspace image, as the RData file folder is not defined.");
		}
		synchronized (rconMonitor) {
			rcon.voidEval("save.image(file = \"" + rDataFile + "\")");
		}
	}

	private void loadImage() throws RserveException {
		if (rDataFile == null) {
			throw new IllegalArgumentException(
					"Cannot load workspace image, as the RData file location is not defined.");
		}
		if (!new File(rDataFile).exists()) {
			throw new IllegalArgumentException("Cannot load workspace image, as the RData file is not defined.");
		}
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

		try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {
			synchronized (rconMonitor) {
				Future<REXP> future = executor.submit(new Callable<REXP>() {
					@Override
					public REXP call() throws Exception {
						return rcon.eval("1+2");
					}
				});
				REXP heartBeat = future.get(timeout, timeUnit);
				if (heartBeat.asDouble() == 3L) {
					beating = true;
				}
			}
		} catch (TimeoutException | InterruptedException e) {
			classLogger.warn("R health check timed out waiting for eval response", e);
		} catch (ExecutionException e) {
			classLogger.warn("R health check eval threw an exception", e);
		} catch (REXPMismatchException e) {
			classLogger.warn("R health check returned an unexpected result type; expected numeric 3.0", e);
		}

		return beating;
	}

	protected boolean isHealthy() {
		return isHealthy(HEALTH_TIMEOUT, HEALTH_TIMEOUT_UNIT);
	}

	@Override
	public boolean isStopped() {
		return this.stoppedR;
	}

	@Override
	public Process getProcess() {
		return process;
	}

}
