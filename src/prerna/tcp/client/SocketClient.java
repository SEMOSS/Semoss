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
	// when set, the broker connects to the worker over this AF_UNIX socket path
	// instead of TCP (used by the namespace sandbox, whose empty netns makes TCP
	// loopback unreachable)
	String udsPath = null;

	Map<String, PayloadStruct> requestMap = new ConcurrentHashMap<>();
	Map<String, PayloadStruct> responseMap = new ConcurrentHashMap<>();
	Map<String, Set<String>> insightToEpoc = new ConcurrentHashMap<>();
	Map<String, Set<String>> jobToEpoc = new ConcurrentHashMap<>();
	Set<String> cancelledEpocs = ConcurrentHashMap.<String>newKeySet();

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

	volatile InputStream is = null;
	volatile OutputStream os = null;
	final Object WRITE_LOCK = new Object();

	Gson gson = new GsonBuilder().disableHtmlEscaping().setObjectToNumberStrategy(ToNumberPolicy.LONG_OR_DOUBLE)
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

	/**
	 * Connect to the worker over an AF_UNIX socket (namespace sandbox mode).
	 *
	 * @param udsPath filesystem path of the worker's Unix domain socket
	 */
	public void connectUds(final String udsPath) {
		this.udsPath = udsPath;
		this.SSL = false;
	}

	/**
	 * Open the transport and populate {@link #is} / {@link #os}. Supports both
	 * TCP ({@link #HOST}/{@link #PORT}) and AF_UNIX ({@link #udsPath}) so the rest
	 * of the client, which only ever touches the streams, is transport-agnostic.
	 *
	 * @throws IOException if the connection cannot be established
	 */
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

	/**
	 * @return a human-readable description of the current transport target
	 */
	protected String transportTarget() {
		return this.udsPath != null ? ("unix:" + this.udsPath) : (this.HOST + ":" + this.PORT);
	}

	@Override
	public void run() {
		// Configure SSL.git
		int attempt = 1;
		int SLEEP_TIME = 800;
		if (Utility.getDIHelperProperty("SLEEP_TIME") != null) {
			SLEEP_TIME = Integer.parseInt(Utility.getDIHelperProperty("SLEEP_TIME"));
		}

		classLogger.info("Trying with sleep time {}", SLEEP_TIME);
		while (!connected && attempt < 6) // I do an attempt here too hmm..
		{
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

				// open TCP or AF_UNIX transport (populates is/os)
				openConnection();
				sch.setClient(this);
				sch.setInputStream(this.is);

				// start this thread
				Thread readerThread = new Thread(sch);
				readerThread.start();

				classLogger.info("Connected to socket server at {}", transportTarget());
				Thread.sleep(100); // sleep some before executing command
				// prime it
				// logger.info("First command.. Prime" + executeCommand("2+2"));
				connected = true;
				ready = true;
				killAll = false;
				synchronized (this) {
					this.notifyAll();
				}
			} catch (Exception ex) {
				attempt++;
				classLogger.info("Attempting connection number {}", attempt);
				// see if sleeping helps ?
				try {
					// sleeping only for 1 second here
					// but the py executor sleeps in 2 second increments
					Thread.sleep(attempt * SLEEP_TIME);
				} catch (Exception ex2) {
					// ignored
				}
			}
		}

		if (attempt >= 6) {
			classLogger.error("Failed to connect to socket server at {} after {} attempts", transportTarget(), attempt);
			killAll = true;
			connected = false;
			ready = false;
			synchronized (this) {
				this.notifyAll();
			}
			throw new IllegalArgumentException("Failed to connect to your isolated analytics engine");
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

		synchronized (ps) // going back to single threaded .. earlier it was ps
		{
			// if(ps.hasReturn)
			// put it into request map
			if (!ps.response) {
				requestMap.put(id, ps);
			}
			classLogger.info("Outgoing epoc {}", ps.epoc);
			writePayload(ps);
			// send the message

			// time to wait = average time * 10
			// if this is a request wait for it
			if (!ps.response) // this is a response to something the socket has asked
			{
				int pollNum = 1; // 1 second
				while (!responseMap.containsKey(ps.epoc) && (pollNum < 10 || ps.longRunning) && !killAll) {
					// logger.info("Checking to see if there was a response");
					try {
						if (pollNum < 10) {
							ps.wait(averageMillis);
						} else { // if(ps.longRunning) // this is to make sure the kill all is being checked
							ps.wait(); // wait eternally - we dont know how long some of the load operations would take
										// besides, I am not sure if the null gets us anything
						}
						pollNum++;
					} catch (InterruptedException e) {
						classLogger.error("Interrupted while waiting for response to epoc: {}", ps.epoc, e);
					}
					/*
					 * // trigger after 400 milliseconds if(pollNum == 2 && !ps.longRunning) {
					 * logger.info("Writing empty message " + ps.epoc); writeEmptyPayload(); }
					 */
				}
				if (!responseMap.containsKey(ps.epoc) && ps.hasReturn) {
					classLogger.info("Timed out waiting for epoc {} method {}", ps.epoc, ps.methodName);

				}
			}

			// after 10 seconds give up
			// printUnprocessed();
			return responseMap.remove(ps.epoc);
		}
	}

	/**
	 * 
	 * @param ps
	 */
	private void writePayload(PayloadStruct ps) {
		byte[] psBytes = FstUtil.packBytes(ps);
		try {
			synchronized (WRITE_LOCK) {
				os.write(psBytes);
			}
		} catch (IOException ex) {
			classLogger.error("Failed to write payload to socket output stream for epoc: {}", ps.epoc, ex);
			crash();
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
				ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();

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
				} finally {
					executor.shutdown();
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
		// this happens when the client has completely crashed
		// make the connected to be false
		// take everything that is waiting on it
		// go through request map and start pushing

		// run as executor since it is synchronized
		// and dont want to get stuck if an issue occurs and the notify never happens
		// we will close and kill process anyway
		ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();

		Callable<String> callableTask = () -> {
			try {
				for (Object k : this.requestMap.keySet()) {
					PayloadStruct ps = this.requestMap.get(k);
					classLogger.debug("Releasing <{}> <{}>", k, ps.methodName);
					ps.ex = "Server has crashed. This happened because you exceeded the memory limits provided or performed an illegal operation. Please relook at your recipe";
					synchronized (ps) {
						ps.notifyAll();
					}
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
		} finally {
			executor.shutdown();
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
					synchronized (lock) {
						lock.notifyAll();
					}
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
