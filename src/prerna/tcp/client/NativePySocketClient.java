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

import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.net.Socket;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.ws.rs.core.StreamingOutput;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.JsonElement;
import com.google.gson.JsonParser;

import prerna.auth.User;
import prerna.om.Insight;
import prerna.om.InsightStore;
import prerna.om.ThreadStore;
import prerna.sablecc2.PixelRunner;
import prerna.sablecc2.PixelStreamUtility;
import prerna.sablecc2.comm.PixelJobManager;
import prerna.sablecc2.comm.PixelJobRunner;
import prerna.sablecc2.comm.PixelJobStatus;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.tcp.PayloadStruct;
import prerna.tcp.client.workers.NativePyEngineWorker;
import prerna.util.Utility;

public class NativePySocketClient extends SocketClient implements Runnable, Closeable {

	private static final Logger classLogger = LogManager.getLogger(NativePySocketClient.class);

	public NativePySocketClient() {
		this.startMdc = new HashMap<>();
	}

	public NativePySocketClient(Map<String, String> startMdc) {
		if (startMdc == null) {
			startMdc = new HashMap<>();
		}
		this.startMdc = startMdc;
	}

	@Override
	public void run() {
		try (var startCtx = org.apache.logging.log4j.CloseableThreadContext.putAll(startMdc)) {
			// there is 2 portions to the run
			// one is before connect
			// one is after. The reason this is done is to avoid an extra handler for
			// information

			// Configure SSL.git
			if (!connected && !killAll) {
				int attempt = 1;
				int SLEEP_TIME = 800;
				if (Utility.getDIHelperProperty("SLEEP_TIME") != null) {
					try {
						SLEEP_TIME = Integer.parseInt(Utility.getDIHelperProperty("SLEEP_TIME"));
					} catch (NumberFormatException e) {
						classLogger.error("Invalid SLEEP_TIME property value: {}",
								Utility.getDIHelperProperty("SLEEP_TIME"), e);
					}
				}

				classLogger.info("Trying with sleep time {}", SLEEP_TIME);
				while (!connected && attempt < 6) {
					try {
						clientSocket = new Socket(this.HOST, this.PORT);
						// pick input and output stream and start the threads
						this.is = clientSocket.getInputStream();
						this.os = clientSocket.getOutputStream();
						classLogger.info("CLIENT Connection complete !!!!!!!");
						// sleep some before executing command
						Thread.sleep(100);

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
					classLogger.error("CLIENT Connection Failed !!!!!!!");
					ready = false;
					synchronized (this) {
						this.notifyAll();
					}
					close();
					throw new IllegalArgumentException("Failed to connect to your isolated analytics engine");
				}
			}

			// this is the read portion
			if (connected) {
				SOCKET_LISTENER: while (!killAll) {
					classLogger.debug("Starting new read iteration in run() loop");
					try {
						String threadName = Thread.currentThread().getName();
						long threadId = Thread.currentThread().threadId();
						classLogger.debug("Socket read thread [{}:{}] attempting to read next message", threadName,
								threadId);

						byte[] length = new byte[4];
						classLogger.debug("Socket read thread [{}:{}] blocking on read()", threadName, threadId);

						// required read to populate the length buffer before wrapping
						@SuppressWarnings("unused") // bytesRead is necessary for proper socket reading
						int bytesRead = is.read(length);

						int size = ByteBuffer.wrap(length).getInt();
						classLogger.debug("Socket read thread [{}:{}] completed read of size header: {} bytes",
								threadName, threadId, size);
						// System.err.println("Incoming data is of size " + size);

						if (size > 0) {
							byte[] msg = new byte[size];
							int size_read = 0;
							classLogger.debug("Starting to read message of size {}", size);
							while (size_read < size) {
								int to_read = size - size_read;
								byte[] newMsg = new byte[to_read];
								int cur_size = is.read(newMsg);
								classLogger.debug("Read chunk of {} bytes, total so far: {}/{}", cur_size,
										size_read + cur_size, size);
								System.arraycopy(newMsg, 0, msg, size_read, cur_size);
								size_read = size_read + cur_size;
								// System.out.println("incoming size " + size + " read size.. " + size_read);
							}

							String message = new String(msg);
							classLogger.debug("Raw message from Python: {}", message);
							// System.err.print(message);
							PayloadStruct ps = gson.fromJson(message, PayloadStruct.class);
							classLogger.debug("Parsed message - epoc: {}, operation: {}, response: {}, interim: {}",
									ps.epoc, ps.operation, ps.response, ps.interim);

							PayloadStruct lock = requestMap.get(ps.epoc);
							classLogger.debug("Incoming payload {}", ps);
							classLogger.debug("Found lock for epoc {}: {}", ps.epoc, lock != null);

							// cancelled operations
							if (ps.operation == PayloadStruct.OPERATION.CANCELLED) {
								classLogger.debug("User cancelled request for epoc: {}", ps.epoc);
							}
							// std out no questions
							else if (ps.operation == PayloadStruct.OPERATION.STDOUT && ps.payload != null
									&& !ps.response) {
								String logMessage = (String) ps.payload[0];
								if (lock != null) {
									exposeLog(logMessage, lock.jobId);
								}
							}

							else if (ps.operation == PayloadStruct.OPERATION.STRUCTURED_STREAM) {
								if (ps.payload != null && ps.payload[0] != null) {
									classLogger.debug("Structed stream: {}", ps.payload[0]);
									if (lock != null && lock.jobId != null) {
										PixelJobManager.getManager().addStreamOut(lock.jobId, (Map) ps.payload[0]);
									}
								}
							}
							// need some way to say this is the output from the actual python vs. something
							// that is a classLogger
							// this is done through interim and operations
							// partial stdout
							// i.e. response is true and it is being sent as a stdout
							else if (ps.operation == PayloadStruct.OPERATION.STDOUT && ps.interim && ps.response) {
								if (ps.payload != null && !((String) ps.payload[0]).equalsIgnoreCase("NONE")) {
									if (lock != null && lock.jobId != null) {
										PixelJobManager.getManager().addPartialOut(lock.jobId, ps.payload[0] + "");
									}
								}
							}
							// this is the response.. i.e. the full response
							else if (ps.response) {
								ps.epoc = ps.epoc.trim();
								classLogger.debug("Processing response for epoc: {}", ps.epoc);
								lock = requestMap.remove(ps.epoc);
								classLogger.debug("Found and removed request lock for epoc {}: {}", ps.epoc,
										lock != null);

								// try to convert it into a full object
								try {
									if (ps.payload[0] != null && ps.payload[0] instanceof String
											&& !((String) ps.payload[0]).isBlank()) {
										Object obj = gson.fromJson((String) ps.payload[0], Object.class);
										ps.payload[0] = obj;
									}
								} catch (Exception ignored) {
									classLogger.warn("Ignoring unable to gson.fromJson() of {}", ps.payload[0]);
								}

								// put it in response
								responseMap.put(ps.epoc, ps);
								classLogger.debug("Added response to responseMap for epoc: {}", ps.epoc);

								if (lock != null) {
									synchronized (lock) {
										classLogger.debug("About to notify waiters for epoc: {}", ps.epoc);
										lock.notifyAll();
										classLogger.debug("Notified waiters for epoc: {}", ps.epoc);

									}
								}
							}
							// this is a request for a reactor
							else if (ps.operation == PayloadStruct.OPERATION.REACTOR) {
								final PayloadStruct finalPs = ps;
								final String jobId = ps.jobId;
								final String sessionId = ps.sessionId;
								final String routeId = ThreadStore.getRouteId();
								final String insightId = finalPs.insightId;
								final String executionInsightId = finalPs.executionInsightId;
								Map<String, String> parentMDC = finalPs.mdc;
								// I'm creating a new thread to run the pixel
								Thread.ofVirtual().start(() -> {
									try (var ctx = org.apache.logging.log4j.CloseableThreadContext.putAll(parentMDC)) {
										classLogger.debug("Starting reactor operation for epoc: {}", finalPs.epoc);
										ByteArrayOutputStream output = new ByteArrayOutputStream();
										String pixelOp = null;
										try {
											Insight insight = InsightStore.getInstance().get(insightId);
											if (insight == null) {
												throw new IllegalArgumentException("Could not find the insight id");
											}
											// set in thread
											ThreadStore.setInsightId(insight.getInsightId());
											ThreadStore.setSessionId(sessionId);
											ThreadStore.setRouteId(routeId);
											ThreadStore.setJobId(jobId);
											// set user based on execution thread if defined
											if (executionInsightId != null) {
												Insight executionInsight = InsightStore.getInstance()
														.get(executionInsightId);
												if (executionInsight != null) {
													ThreadStore.setUser(executionInsight.getUser());
												}
											}
											// default to the user for the insight space
											if (ThreadStore.getUser() == null) {
												ThreadStore.setUser(insight.getUser());
											}

											pixelOp = (String) finalPs.payload[0];
											if (!(pixelOp = pixelOp.trim()).endsWith(";")) {
												pixelOp += ";";
											}
											PixelRunner pixelRunner = insight.runPixel(pixelOp);
											StreamingOutput streamedOutput = PixelStreamUtility
													.collectPixelData(pixelRunner, null);
											streamedOutput.write(output);
											JsonElement json = JsonParser
													.parseString(new String(output.toByteArray(), "UTF-8"));
											finalPs.payload = new Object[] { json };
											finalPs.response = true;
											executeCommand(finalPs);
										} catch (Exception e) {
											classLogger.error(
													"Error executing pixel operation in reactor thread for epoc: {}",
													finalPs.epoc, e);
											finalPs.response = true;
											String errorMessage = "An error occurred running the pixel = " + pixelOp;
											if (e.getMessage() != null) {
												errorMessage += ". Error message = " + e.getMessage();
											}
											finalPs.ex = errorMessage;
											executeCommand(finalPs);
										} finally {
											try {
												output.close();
											} catch (IOException e) {
												classLogger.error(
														"Error closing output stream in reactor thread for epoc: {}",
														finalPs.epoc, e);
											}
										}
									}
								});
							}
							// this is a request
							else if (ps.operation == PayloadStruct.OPERATION.ENGINE) {
								// classLogger.info("reverse request for data");
								// this is a request we need to process
								// need a way here to also push the payload classes
								// will come to it in a bit
								// clean up the payload struct a little
								ps = convertPayloadClasses(ps);
								processEngineRequest(ps);
							}
							// unhandled pieces.. nothing we can do here.. just give the response back
							// so we dont choke the thread
							else {
								classLogger.info("Message not handled by py server");
								lock = requestMap.remove(ps.epoc);
								responseMap.put(ps.epoc, ps);
								if (lock != null) {
									synchronized (lock) {
										lock.notifyAll();
									}
								}
							}
						} else {
							crash();
							break SOCKET_LISTENER;
						}
					} catch (SocketException ex1) {
						crash();
						break SOCKET_LISTENER;
					} catch (Exception ex) {
						classLogger.error("Unexpected error in socket listener loop", ex);
					}
				}
			}
		} finally {
			classLogger.info("Attemping to gracefully shutdown the socket server");
			if (this.cpw != null) {
				this.cpw.shutdown(true);
			}
			classLogger.warn("NativePySocketClient is disconnected");
			// this will set connected to false
			// note shutdown must happen before we flip to connected false
			// in order to properly force cleanup of streams
			this.close();
		}
	}

	/**
	 * 
	 * @param ps
	 */
	private void processEngineRequest(PayloadStruct ps) {
		String insightId = ps.insightId;
		if (insightId == null || (insightId = insightId.trim()).isEmpty() || insightId.equals("${i}")) {
			ps.response = true;
			ps.ex = "Insight Id is undefined or null";
			// return the error
			executeCommand(ps);
			return;
		}
		Insight insight = InsightStore.getInstance().get(insightId);
		if (insight == null) {
			ps.response = true;
			ps.ex = "Could not find the insight id";
			// return the error
			executeCommand(ps);
			return;
		}
		User user = null;
		String executionInsightId = ps.executionInsightId;
		if (executionInsightId != null) {
			Insight executionInsight = InsightStore.getInstance().get(executionInsightId);
			if (executionInsight != null) {
				ThreadStore.setUser(executionInsight.getUser());
			}
		}
		if (user == null) {
			user = insight.getUser();
		}
		if (user == null) {
			ps.response = true;
			ps.ex = "There is no user associated with this insight id";
			// return the error
			executeCommand(ps);
			return;
		} else {
			NativePyEngineWorker worker = new NativePyEngineWorker(user, ps, insight);
			worker.run();
			executeCommand(worker.getOutput());
		}
	}

	/**
	 * 
	 * @param input
	 * @return
	 */
	private PayloadStruct convertPayloadClasses(PayloadStruct input) {
		if (input.payloadClassNames != null) {
			input.payloadClasses = new Class[input.payloadClassNames.length];
			for (int classIndex = 0; classIndex < input.payloadClassNames.length; classIndex++) {
				try {
					String className = input.payloadClassNames[classIndex];
					input.payloadClasses[classIndex] = Class.forName(className);
					if (input.payloadClasses[classIndex] == Insight.class) {
						String insightId = input.insightId;
						Insight insight = InsightStore.getInstance().get(insightId);
						input.payload[classIndex] = insight;
					}
				} catch (ClassNotFoundException e) {
					classLogger.error("Could not find class: {}", input.payloadClassNames[classIndex], e);
				}
			}
		}
		return input;
	}

	/**
	 * This is the method that pushes to the front end when output happens
	 * 
	 * @param data
	 * @param insightId
	 */
	private void exposeLog(String data, String jobId) {
		classLogger.debug("Exposing log to jobId = '{}' with data = {}", jobId, data);
		if (jobId != null && data != null) {
			PixelJobManager.getManager().addStdOut(jobId, data);
		} else {
			// 2025-07-08
			// currently insights for the model py translator is not in store
			classLogger.debug("JobId = '{}' is not in insight store", jobId);
		}
	}

	@Override
	public void interruptInsight(String insightId) {
		interruptInsightJob(insightId, null);
	}

	@Override
	public void interruptInsightJob(String insightId, String jobId) {
		// Always cancel local waiters first.
		super.interruptInsightJob(insightId, jobId);

		if ((insightId == null || insightId.isBlank()) && (jobId == null || jobId.isBlank())) {
			return;
		}
		if (!this.connected || this.killAll) {
			return;
		}

		// Best-effort remote interrupt so the Python process can stop the currently
		// running execution without killing the socket server process.
		PayloadStruct ps = new PayloadStruct();
		ps.epoc = "pi" + count.getAndIncrement();
		ps.operation = PayloadStruct.OPERATION.INSIGHT;
		ps.methodName = "interruptInsight";
		ps.payload = new Object[] { "INTERRUPT_INSIGHT" };
		ps.hasReturn = false;
		ps.longRunning = false;
		ps.insightId = insightId;
		ps.executionInsightId = insightId;
		ps.jobId = jobId;
		writePayload(ps);
	}

	@Override
	public void interruptInsight(String insightId, String jobId) {
		interruptInsightJob(insightId, jobId);
	}

	@Override
	public Object executeCommand(PayloadStruct ps) {
		String threadName = Thread.currentThread().getName();
		long threadId = Thread.currentThread().threadId();
		classLogger.debug("Entering executeCommand for epoc: {} on thread: {} (ID: {})", ps.epoc, threadName, threadId);
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
		if (ps.insightId != null) {
			addEpocForInsight(ps.insightId, ps.epoc);
		}
		if (ps.executionInsightId != null && !ps.executionInsightId.equals(ps.insightId)) {
			addEpocForInsight(ps.executionInsightId, ps.epoc);
		}
		if (ps.jobId != null) {
			addEpocForJob(ps.jobId, ps.epoc);
		}
		ps.longRunning = true;

		try {
			synchronized (ps) {
				classLogger.debug("Inside synchronized block for epoc: {} on thread: {} (ID: {})", ps.epoc, threadName,
						threadId);

				if (!ps.response) {
					this.requestMap.put(id, ps);
				}
				writePayload(ps);
				classLogger.debug("outgoing payload {}", ps.epoc);

				// send the message
				// time to wait = average time * 10
				// if this is a request wait for it

				int maxWait = 1_000;
				if (!ps.response) // this is a response to something the socket has asked
				{
					int pollNum = 1;
					while (!responseMap.containsKey(ps.epoc) && (pollNum < maxWait || ps.longRunning) && !killAll
							&& !cancelledEpocs.contains(ps.epoc)) {
						classLogger.debug("Thread {} waiting for response to epoc: {} (poll #{})",
								Thread.currentThread().threadId(), ps.epoc, pollNum);
						try {
							classLogger.debug("I'm looking for epoc{}", ps.epoc);
							if (pollNum < maxWait) {
								ps.wait(this.averageMillis);
							} else {
								classLogger.debug("Im about to wait eternally for epoc {}", ps.epoc);
								// wait eternally - we dont know how long some of the load operations would take
								// besides
								// I am not sure if the null gets us anything
								ps.wait();
							}
							pollNum++;
						} catch (InterruptedException e) {
							boolean cancelled = cancelledEpocs.contains(ps.epoc);
							if (!cancelled && ps.jobId != null) {
								PixelJobRunner jobRunner = PixelJobManager.getManager().getJob(ps.jobId);
								cancelled = jobRunner != null
										&& jobRunner.getPixelJobStatus() == PixelJobStatus.CANCELED;
							}

							if (cancelled) {
								classLogger.debug("Interrupted due to cancel for epoc {}", ps.epoc);
								break; // let existing cancelledEpocs/job handling throw cancel response
							}

							classLogger.warn("Interrupted while waiting for epoc {}", ps.epoc, e);
						}
					}
					if (cancelledEpocs.contains(ps.epoc)) {
						cancelledEpocs.remove(ps.epoc);
						classLogger.info("Cancelled epoc {} {}", ps.epoc, ps.methodName);
						throw new SemossPixelException("The request was cancelled by the user");
					} else if (!responseMap.containsKey(ps.epoc) && ps.hasReturn) {
						classLogger.info("Timed out for epoc {} {}", ps.epoc, ps.methodName);
					}
				}

				return responseMap.remove(ps.epoc);
			}
		} finally {
			removeEpocForInsight(ps.insightId, ps.epoc);
			if (ps.executionInsightId != null && !ps.executionInsightId.equals(ps.insightId)) {
				removeEpocForInsight(ps.executionInsightId, ps.epoc);
			}
			removeEpocForJob(ps.jobId, ps.epoc);
		}
	}

	/**
	 * 
	 * @param ps
	 */
	private void writePayload(PayloadStruct ps) {
		classLogger.debug("Starting writePayload for epoc: {}", ps.epoc);
		ps.payloadClasses = null;
		try {
			String jsonPS = gson.toJson(ps);
			byte[] psBytes = pack(jsonPS, ps.epoc);
			try {
				synchronized (WRITE_LOCK) {
					classLogger.debug("About to write to output stream for epoc: {}", ps.epoc);
					os.write(psBytes);
					classLogger.debug("Successfully wrote to output stream for epoc: {}", ps.epoc);
				}
			} catch (IOException ex) {
				classLogger.error("Failed to write payload to output stream for epoc: {}", ps.epoc, ex);
			}
		} catch (Exception ex) {
			classLogger.error("Unexpected error serializing payload for epoc: {}", ps.epoc, ex);
		}
	}

	/**
	 * 
	 * @param message
	 * @param epoc
	 * @return
	 */
	public byte[] pack(String message, String epoc) {
		byte[] psBytes = message.getBytes(StandardCharsets.UTF_8);

		// get the length
		int length = psBytes.length;

		// System.err.println("Packing with length " + length);

		// make this into array
		byte[] lenBytes = ByteBuffer.allocate(4).putInt(length).array();

		byte[] epocBytes = ByteBuffer.allocate(20).put(epoc.getBytes(StandardCharsets.UTF_8)).array();

		// pack both of these
		byte[] finalByte = new byte[psBytes.length + lenBytes.length + epocBytes.length];

		for (int lenIndex = 0; lenIndex < lenBytes.length; lenIndex++) {
			finalByte[lenIndex] = lenBytes[lenIndex];
		}

		// write the epoc bytes
		for (int lenIndex = 0; lenIndex < epocBytes.length; lenIndex++) {
			finalByte[lenIndex + lenBytes.length] = epocBytes[lenIndex];
		}

		for (int lenIndex = 0; lenIndex < psBytes.length; lenIndex++) {
			finalByte[lenIndex + lenBytes.length + epocBytes.length] = psBytes[lenIndex];
		}

		return finalByte;
	}

	/**
	 * 
	 */
	@Override
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
	@Override
	public boolean stopServer() {
		try {
			if (isConnected()) {
				ExecutorService executor = Executors.newSingleThreadExecutor();

				Callable<Boolean> callableTask = () -> {
					PayloadStruct ps = new PayloadStruct();
					ps.epoc = "stop_all";
					ps.hasReturn = false;
					ps.methodName = "CLOSE_ALL_LOGOUT<o>";
					ps.payload = new String[] { "CLOSE_ALL_LOGOUT<o>" };
					ps.operation = PayloadStruct.OPERATION.CMD;
					writePayload(ps);
					return true;
				};

				Future<Boolean> future = executor.submit(callableTask);
				try {
					boolean result = future.get(5, TimeUnit.SECONDS);
					classLogger.info("Stop socket result = {}", result);
					return result;
				} catch (TimeoutException e) {
					classLogger.warn("Not able to release the payload structs within a timely fashion");
					future.cancel(true);
					return false;
				} catch (InterruptedException | ExecutionException e) {
					Thread.currentThread().interrupt();
					classLogger.error("Interrupted or execution failure during stop server", e);
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
	@Override
	public void crash() {
		// this happens when the client losses connection to the server
		classLogger.warn("NativePySocketClient is disconnected from server");

		// run as executor since it is synchronized
		// and dont want to get stuck if an issue occurs and the notify never happens
		// we will close and kill process anyway

		ExecutorService executor = Executors.newSingleThreadExecutor();
		Callable<String> callableTask = () -> {
			try {
				for (Object k : this.requestMap.keySet()) {
					PayloadStruct ps = this.requestMap.get(k);
					classLogger.debug("Releasing <{}> <{}>", k, ps.methodName);
					ps.ex = "Client is disconnected from the server.";
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
			Thread.currentThread().interrupt();
			classLogger.error("Interrupted or execution failure during crash", e);
		} finally {
			executor.shutdown();
		}

		this.close();
	}

}
