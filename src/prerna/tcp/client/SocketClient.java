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
package prerna.tcp.client;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.net.UnixDomainSocketAddress;
import java.nio.channels.Channels;
import java.nio.channels.SocketChannel;
import java.time.Duration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.ToNumberPolicy;

import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import prerna.auth.User;
import prerna.om.ClientProcessWrapper;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.tcp.PayloadStruct;
import prerna.util.FstUtil;
import prerna.util.Settings;
import prerna.util.Utility;

public class SocketClient implements Runnable, Closeable {

	private static final Logger classLogger = LogManager.getLogger(SocketClient.class);

	String HOST = null;
	int PORT = -1;
	boolean SSL = false;
	String udsPath = null;

	Map<String, PayloadStruct> requestMap = new ConcurrentHashMap<>();
	Map<String, PayloadStruct> responseMap = new ConcurrentHashMap<>();
	Map<String, Set<String>> insightToEpoc = new ConcurrentHashMap<>();
	Map<String, Set<String>> jobToEpoc = new ConcurrentHashMap<>();
	Set<String> cancelledEpocs = ConcurrentHashMap.<String>newKeySet();

	// Hard cap on how long a caller blocks waiting for a python response before it
	// gives up. We never wait forever: after this the caller surfaces a timeout,
	// but
	// python keeps running (we don't kill the process), so a script that persists
	// its
	// own results can still finish and be retrieved later.
	protected static final Duration MAX_RESPONSE_WAIT = Duration.ofHours(24);

	volatile boolean ready = false;
	volatile boolean connected = false;
	AtomicInteger count = new AtomicInteger(0);
	long averageMillis = 200;
	// use this if the server is dead
	volatile boolean killAll = false;
	User user;

	Map<String, String> startMdc = null;

	Socket clientSocket = null;
	SocketChannel udsChannel = null;
	SocketClientHandler sch = new SocketClientHandler();

	private final ReentrantLock readinessLock = new ReentrantLock();
	private final Condition readinessChanged = readinessLock.newCondition();

	volatile InputStream is = null;
	volatile OutputStream os = null;
	final ReentrantLock WRITE_LOCK = new ReentrantLock();

	final Gson GSON = new GsonBuilder().disableHtmlEscaping().setObjectToNumberStrategy(ToNumberPolicy.LONG_OR_DOUBLE)
			.create();

	ClientProcessWrapper cpw = null;

	public SocketClient() {
		this.startMdc = new HashMap<>();
	}

	public SocketClient(Map<String, String> startMdc) {
		this.startMdc = startMdc;
	}

	/**
	 * 
	 * @param HOST
	 * @param PORT
	 * @param SSL
	 */
	public void connect(final String HOST, final int PORT, final boolean SSL) {
		this.HOST = HOST;
		this.PORT = PORT;
		this.SSL = SSL;
	}

	public void connectUds(final String udsPath) {
		this.udsPath = udsPath;
		this.SSL = false;
	}

	protected void openConnection() throws IOException {
		if (this.udsPath != null) {
			UnixDomainSocketAddress address = UnixDomainSocketAddress.of(this.udsPath);
			this.udsChannel = SocketChannel.open(address);
			this.is = Channels.newInputStream(this.udsChannel);
			this.os = Channels.newOutputStream(this.udsChannel);
		} else {
			this.clientSocket = new Socket(this.HOST, this.PORT);
			this.is = this.clientSocket.getInputStream();
			this.os = this.clientSocket.getOutputStream();
		}
	}

	protected String transportTarget() {
		return this.udsPath != null ? ("unix:" + this.udsPath) : (this.HOST + ":" + this.PORT);
	}

	@Override
	public void run() {
		int attempt = 1;
		int SLEEP_TIME = 800;
		if (Utility.getDIHelperProperty("SLEEP_TIME") != null) {
			SLEEP_TIME = Integer.parseInt(Utility.getDIHelperProperty("SLEEP_TIME"));
		}

		classLogger.info("Trying with sleep time {}", SLEEP_TIME);
		while (!connected && attempt < 6) {
			try {
				final SslContext sslCtx;
				if (SSL) {
					sslCtx = SslContextBuilder.forClient().trustManager(InsecureTrustManagerFactory.INSTANCE).build();
				} else {
					sslCtx = null;
				}

				// Configure the client.
				boolean blocking = Utility.getDIHelperProperty(Settings.BLOCKING) != null
						&& Utility.getDIHelperProperty(Settings.BLOCKING).equalsIgnoreCase("true");

				openConnection();
				sch.setClient(this);
				sch.setInputStream(this.is);

				// start this thread
				Thread readerThread = new Thread(sch);
				readerThread.start();

				classLogger.info("Connected to socket server at {}", transportTarget());
				Thread.sleep(100); // brief pause before issuing commands
				connected = true;
				ready = true;
				killAll = false;
				signalReadinessChanged();
			} catch (Exception ex) {
				attempt++;
				classLogger.info("Attempting connection number {}", attempt);
				try {
					Thread.sleep(attempt * SLEEP_TIME);
				} catch (InterruptedException ex2) {
					Thread.currentThread().interrupt();
				}
			}
		}

		if (attempt >= 6) {
			classLogger.error("Failed to connect to socket server at {} after {} attempts", transportTarget(), attempt);
			killAll = true;
			connected = false;
			ready = false;
			signalReadinessChanged();
			throw new IllegalArgumentException("Failed to connect to your isolated analytics engine");
		}
	}

	public void awaitReadyOrKill() {
		awaitReadyOrKill(60_000L, 1_000L);
	}

	public void awaitReadyOrKill(long timeoutMillis, long pollIntervalMillis) {
		long waitDeadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
		long pollNanos = TimeUnit.MILLISECONDS.toNanos(pollIntervalMillis);

		readinessLock.lock();
		try {
			while (!this.ready && !this.killAll) {
				long remainingNanos = waitDeadline - System.nanoTime();
				if (remainingNanos <= 0) {
					throw new IllegalArgumentException(
							"Timed out waiting for isolated analytics engine socket client to become ready");
				}
				try {
					long waitNanos = Math.min(pollNanos, remainingNanos);
					readinessChanged.await(waitNanos, TimeUnit.NANOSECONDS);
				} catch (InterruptedException e) {
					Thread.currentThread().interrupt();
					classLogger.error("Interrupted while waiting for socket client readiness", e);
					throw new IllegalStateException("Interrupted while waiting for socket client readiness", e);
				}
			}
		} finally {
			readinessLock.unlock();
		}

		if (this.killAll) {
			throw new IllegalArgumentException("Failed to connect to your isolated analytics engine");
		}
	}

	protected void signalReadinessChanged() {
		readinessLock.lock();
		try {
			readinessChanged.signalAll();
		} finally {
			readinessLock.unlock();
		}
	}

	/**
	 * 
	 * @param ps
	 * @return
	 */
	public Object executeCommand(PayloadStruct ps) {
		if (killAll) {
			throw new SemossPixelException(
					"Analytic engine is no longer available. This happened because you exceeded the memory limits provided or performed an illegal operation. Please relook at your recipe");
		}

		if (!connected) {
			throw new SemossPixelException("Your micro-process is not available. Please logout and try again. !");
		}

		String id = ps.epoc;
		if (!ps.response || id == null) {
			id = "ps" + count.getAndIncrement();
			ps.epoc = id;
		}
		ps.longRunning = true;

		// ReentrantLock + Condition (not synchronized/wait) so a virtual thread
		// blocked here does not pin its carrier while it waits for the response
		ps.lockResponse();
		try {
			if (!ps.response) {
				requestMap.put(id, ps);
			}
			classLogger.info("Outgoing epoc {}", ps.epoc);
			writePayload(ps);

			// if this is a request, wait for the response to come back
			if (!ps.response) {
				long waitDeadline = System.nanoTime() + MAX_RESPONSE_WAIT.toNanos();
				int pollNum = 1;
				while (!responseMap.containsKey(ps.epoc) && (pollNum < 10 || ps.longRunning) && !killAll
						&& System.nanoTime() < waitDeadline) {
					try {
						if (pollNum < 10) {
							ps.awaitResponse(averageMillis, TimeUnit.MILLISECONDS);
						} else {
							// wait for the response, but no longer than the time remaining
							// until the cap - we don't know how long load operations take, but
							// we don't wait forever either
							long remainingNanos = waitDeadline - System.nanoTime();
							if (remainingNanos > 0) {
								ps.awaitResponse(remainingNanos, TimeUnit.NANOSECONDS);
							}
						}
						pollNum++;
					} catch (InterruptedException e) {
						classLogger.error("Interrupted while waiting for response to epoc: {}", ps.epoc, e);
					}
				}
				if (!responseMap.containsKey(ps.epoc) && System.nanoTime() >= waitDeadline) {
					// hit the hard cap - stop waiting but leave python running so a script
					// that persists its own results can still finish
					this.requestMap.remove(ps.epoc);
					classLogger.warn(
							"Stopped waiting for epoc {} method {} after the {}h max wait; python continues running in the background",
							ps.epoc, ps.methodName, MAX_RESPONSE_WAIT.toHours());
					throw new SemossPixelException("This execution exceeded the maximum wait time of "
							+ MAX_RESPONSE_WAIT.toHours()
							+ " hours. The python process is still running in the background - if your script persists its results, you can retrieve them later.");
				} else if (!responseMap.containsKey(ps.epoc) && ps.hasReturn) {
					classLogger.info("Timed out waiting for epoc {} method {}", ps.epoc, ps.methodName);
				}
			}

			return responseMap.remove(ps.epoc);
		} finally {
			ps.unlockResponse();
		}
	}

	/**
	 * 
	 * @param ps
	 */
	private void writePayload(PayloadStruct ps) {
		byte[] psBytes = FstUtil.packBytes(ps);
		WRITE_LOCK.lock();
		try {
			os.write(psBytes);
		} catch (IOException ex) {
			classLogger.error("Failed to write payload to socket output stream for epoc: {}", ps.epoc, ex);
			crash();
		} finally {
			WRITE_LOCK.unlock();
		}
	}

	/**
	 * 
	 */
	public void writeReleaseAllPayload() {
		PayloadStruct ps = new PayloadStruct();
		ps.epoc = Utility.getRandomString(8);
		ps.methodName = "RELEASE_ALL";
		writePayload(ps);
	}

	/**
	 * 
	 * @return
	 */
	public boolean stopServer() {
		try {
			if (isConnected()) {
				try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {
					Callable<Boolean> callableTask = () -> {
						PayloadStruct ps = new PayloadStruct();
						ps.methodName = "CLOSE_ALL_LOGOUT<o>";
						ps.payload = new String[] { "CLOSE_ALL_LOGOUT<o>" };
						writePayload(ps);
						return true;
					};

					Future<Boolean> future = executor.submit(callableTask);
					try {
						// wait 1 minute at most
						boolean result = future.get(60, TimeUnit.SECONDS);
						classLogger.info("Stop PyServe result = {}", result);
						return result;
					} catch (TimeoutException e) {
						classLogger.warn("Not able to release the payload structs within a timely fashion");
						future.cancel(true);
						return false;
					} catch (InterruptedException | ExecutionException e) {
						classLogger.error("Error stopping socket server at {}:{}", this.HOST, this.PORT, e);
						return false;
					}
				}
			} else {
				return true;
			}
		} finally {
			// always call close on the IO
			close();
		}
	}

	/**
	 * 
	 */
	public void crash() {
		// the client has lost the server - release everything waiting on a response.
		// run on a separate executor so a stuck signal can't block us; we close and
		// kill the process regardless
		try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {
			Callable<String> callableTask = () -> {
				try {
					for (Object k : this.requestMap.keySet()) {
						PayloadStruct ps = this.requestMap.get(k);
						classLogger.debug("Releasing <{}> <{}>", k, ps.methodName);
						ps.ex = "Server has crashed. This happened because you exceeded the memory limits provided or performed an illegal operation. Please relook at your recipe";
						ps.signalResponse();
					}
				} catch (Exception e) {
					classLogger.error("Error releasing pending payload structs during crash", e);
				}
				return "Successfully released the payload structs";
			};

			Future<String> future = executor.submit(callableTask);
			try {
				// wait 1 minute at most
				String result = future.get(60, TimeUnit.SECONDS);
				classLogger.info(result);
			} catch (TimeoutException e) {
				classLogger.warn("Not able to release the payload structs within a timely fashion");
				future.cancel(true);
			} catch (InterruptedException | ExecutionException e) {
				classLogger.error("Error waiting for crash cleanup to complete", e);
			}
		}

		this.close();
		throw new SemossPixelException(
				"Analytic engine is no longer available. This happened because you exceeded the memory limits provided or performed an illegal operation. Please relook at your recipe");
	}

	@Override
	public void close() {
		if (this.requestMap != null) {
			this.requestMap.clear();
		}
		if (this.responseMap != null) {
			this.responseMap.clear();
		}
		if (this.insightToEpoc != null) {
			this.insightToEpoc.clear();
		}
		if (this.jobToEpoc != null) {
			this.jobToEpoc.clear();
		}
		closeStream(this.os);
		closeStream(this.is);
		closeStream(this.clientSocket);
		closeStream(this.udsChannel);
		this.killAll = true;
		this.connected = false;
	}

	/**
	 * 
	 * @param insightId
	 * @param epoc
	 */
	void addEpocForInsight(String insightId, String epoc) {
		Set<String> epocs = this.insightToEpoc.computeIfAbsent(insightId, x -> ConcurrentHashMap.<String>newKeySet());
		epocs.add(epoc);
	}

	/**
	 * 
	 * @param insightId
	 * @param epoc
	 */
	void removeEpocForInsight(String insightId, String epoc) {
		if (insightId != null) {
			Set<String> epocs = this.insightToEpoc.get(insightId);
			if (epocs != null) {
				epocs.remove(epoc);
			}
		}
	}

	void addEpocForJob(String jobId, String epoc) {
		if (jobId == null || epoc == null) {
			return;
		}
		Set<String> epocs = this.jobToEpoc.computeIfAbsent(jobId, x -> ConcurrentHashMap.<String>newKeySet());
		epocs.add(epoc);
	}

	void removeEpocForJob(String jobId, String epoc) {
		if (jobId == null || epoc == null) {
			return;
		}
		Set<String> epocs = this.jobToEpoc.get(jobId);
		if (epocs != null) {
			epocs.remove(epoc);
		}
	}

	/**
	 * 
	 * @param insightId
	 */
	public void interruptInsight(String insightId) {
		Set<String> epocs = new HashSet<>();
		if (insightId != null) {
			Set<String> insightEpocs = this.insightToEpoc.get(insightId);
			if (insightEpocs != null) {
				epocs.addAll(insightEpocs);
			}
		}
		interruptEpocs(epocs);
	}

	/**
	 * Interrupt a specific job execution. If {@code jobId} is null/blank, this will
	 * interrupt all jobs for the insight.
	 * 
	 * @param insightId
	 * @param jobId
	 */
	public void interruptInsightJob(String insightId, String jobId) {
		if (jobId == null || jobId.isBlank()) {
			interruptInsight(insightId);
			return;
		}

		Set<String> epocs = new HashSet<>();
		Set<String> jobEpocs = this.jobToEpoc.get(jobId);
		if (jobEpocs != null) {
			epocs.addAll(jobEpocs);
		}
		interruptEpocs(epocs);
	}

	/**
	 * Backward-compatible overload. Prefer
	 * {@link #interruptInsightJob(String, String)}.
	 */
	public void interruptInsight(String insightId, String jobId) {
		interruptInsightJob(insightId, jobId);
	}

	private void interruptEpocs(Set<String> epocs) {
		if (!epocs.isEmpty()) {
			this.cancelledEpocs.addAll(epocs);
			for (String epoc : epocs) {
				PayloadStruct lock = this.requestMap.remove(epoc);
				if (lock != null) {
					lock.signalResponse();
				}
			}
		}
	}

	/**
	 * 
	 * @param closeThis
	 */
	void closeStream(Closeable closeThis) {
		if (closeThis != null) {
			try {
				closeThis.close();
			} catch (IOException e) {
				classLogger.error("Error closing resource in socket client", e);
			}
		}
	}

	/**
	 * 
	 * @param user
	 */
	public void setUser(User user) {
		this.user = user;
	}

	/**
	 * 
	 * @return
	 */
	public User getUser() {
		return this.user;
	}

	/**
	 * 
	 * @return
	 */
	public ClientProcessWrapper getCpw() {
		return cpw;
	}

	/**
	 * 
	 * @param cpw
	 */
	public void setCpw(ClientProcessWrapper cpw) {
		this.cpw = cpw;
	}

	/**
	 * 
	 * @return
	 */
	public boolean isConnected() {
		return this.connected;
	}

	/**
	 * 
	 * @return
	 */
	public boolean isKillAll() {
		return killAll;
	}

	/**
	 * 
	 * @return
	 */
	public boolean isReady() {
		return this.ready;
	}

}
