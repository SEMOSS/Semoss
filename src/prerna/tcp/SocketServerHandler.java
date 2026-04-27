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
package prerna.tcp;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.ServerSocket;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.openqa.selenium.chrome.ChromeDriver;
import org.rosuda.REngine.Rserve.RConnection;

import prerna.engine.api.IDatabaseEngine;
import prerna.om.Insight;
import prerna.project.impl.Project;
import prerna.reactor.IReactor;
import prerna.reactor.frame.r.util.AbstractRJavaTranslator;
import prerna.reactor.frame.r.util.RJavaJriTranslator;
import prerna.reactor.frame.r.util.RJavaRserveTranslator;
import prerna.sablecc2.om.NounStore;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.CmdExecUtil;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.FstUtil;
import prerna.util.TCPChromeDriverUtility;
import prerna.util.Utility;

/**
 * Handles socket payload processing for a connected client and coordinates
 * request/response routing between SEMOSS core and runtime components.
 */
public class SocketServerHandler implements Runnable {

	public static Logger classLogger = null;

	boolean test = false;

	private int offset = 4;
	private byte[] lenBytes = null;
	private byte[] curBytes = null;
	private int bytesReadSoFar = 0;
	private int lenBytesReadSoFar = 0;
	private boolean done = false;
	// processes one payload and moves to the next one. This is how it currently
	// behaves
	private boolean blocking = false;
	private long averageMillis = 200;

	ServerSocket socket = null;
	// Back-reference to the owning SocketServer; used for crash signaling.
	SocketServer server = null;
	OutputStream os = null;
	InputStream is = null;
	String mainFolder = null;

	private volatile RConnection retCon = null;
	private final Object translatorInitLock = new Object();

	private final Map<String, AbstractRJavaTranslator> rtMap = new ConcurrentHashMap<String, AbstractRJavaTranslator>();
	private Map<String, Insight> insightMap = new ConcurrentHashMap<String, Insight>();
	private Map<String, Project> projectMap = new ConcurrentHashMap<String, Project>();
	private Map<String, CmdExecUtil> cmdMap = new ConcurrentHashMap<String, CmdExecUtil>();

	private final Map<String, PayloadStruct> incoming = new ConcurrentHashMap<String, PayloadStruct>();
	private final Map<String, PayloadStruct> outgoing = new ConcurrentHashMap<String, PayloadStruct>();

	private int curEpoc = 1;

	/**
	 * Sets the logger used by handler instances.
	 * 
	 * @param classLogger logger to use for handler lifecycle and payload processing
	 */
	public void setLogger(Logger classLogger) {
		SocketServerHandler.classLogger = classLogger;
	}

	/**
	 * Processes an incoming payload and returns the response payload that should be
	 * sent back over the socket.
	 * 
	 * @param ps incoming payload
	 * @return response payload, or {@code null} when processing fails
	 */
	public PayloadStruct getFinalOutput(PayloadStruct ps) {
		try {
			// System.err.println("Received For Processing " + ps.methodName + " bytes : " +
			// totalBytes + " Epoc " + ps.epoc);
			// classLogger.info("Received For Processing " + ps.methodName + " bytes : " +
			// totalBytes + " Epoc " + ps.epoc);
			// unprocessed.put(ps.epoc, ps);
			// attemptCount.put(ps.epoc, 1);

			incoming.put(ps.epoc, ps);
			ps.response = true;
			outgoing.put(ps.epoc, ps);

			// System.out.println("Getting final output for " + ps.methodName);
			classLogger.info("Getting final output for method '{}'", ps.methodName);

			//// System.err.println("Payload set to " + ps);
			if (ps.methodName.equalsIgnoreCase("EMPTYEMPTYEMPTY")) { // trigger message ignore
				return ps;
			}
			if (ps.methodName.equalsIgnoreCase("CLOSE_ALL_LOGOUT<o>")) { // we are done kill everything
				cleanUp();
			}
			if (ps.methodName.equalsIgnoreCase("RELEASE_ALL")) { // we are done kill everything
				releaseAll();
				return ps;
			}

			if (ps.operation == PayloadStruct.OPERATION.R) {
				try {
					AbstractRJavaTranslator translator = getTranslator(ps.env);
					Method method = findRMethod(translator, ps.methodName, ps.payloadClasses);
					Object output = runMethodR(translator, method, ps.payload);
					if (output != null) {
						// System.out.println("Output is not null - R");
						classLogger.info("Output is not null - R");
					}
					Object[] retObject = new Object[1];
					retObject[0] = output;
					ps.payload = retObject;
					ps.processed = true;
					ps.response = true;
				} catch (InvocationTargetException ex) {
					classLogger.error("Invocation error while executing R operation '{}'", ps.methodName, ex);
					classLogger.debug("R operation '{}' failed with exception '{}'", ps.methodName, ex.toString());
					// System.err.println("Method.. " + ps.methodName);
					ps.ex = ExceptionUtils.getStackTrace(ex);
				} catch (Exception ex) {
					classLogger.error("Error while executing R operation '{}'", ps.methodName, ex);
					classLogger.debug("R operation '{}' failed with exception '{}'", ps.methodName, ex.toString());
					// System.err.println("Method.. " + ps.methodName);
					ps.ex = ExceptionUtils.getStackTrace(ex);
				}
				return ps;
			} else if (ps.operation == PayloadStruct.OPERATION.CHROME) {
				try {
					Method method = findChromeMethod(ps.methodName, ps.payloadClasses);
					Object output = runMethodChrome(method, ps.payload);
					if (output != null) {
						classLogger.info("Output is not null - CHROME");
					}
					if (output instanceof ChromeDriver) {
						output = new Object();
					}
					if (output instanceof String) {
						classLogger.info("CHROME operation '{}' returned String output: {}", ps.methodName, output);
					}
					Object[] retObject = new Object[1];
					retObject[0] = output;
					ps.payload = retObject;
					ps.processed = true;
				} catch (Exception ex) {
					classLogger.error("Error while executing CHROME operation '{}'", ps.methodName, ex);
					// System.err.println("Method.. " + ps.methodName);
					ps.ex = ExceptionUtils.getStackTrace(ex);
					// TCPChromeDriverUtility.quit("stop");
				}
				return ps;
			} else if (ps.operation == PayloadStruct.OPERATION.ECHO) {
				try {
					Method method = findChromeMethod(ps.methodName, ps.payloadClasses);
					Object output = ps.payload[0];
					Object[] retObject = new Object[1];
					retObject[0] = output;
					ps.payload = retObject;
					ps.processed = true;
				} catch (Exception ex) {
					classLogger.error("Error while executing ECHO operation '{}'", ps.methodName, ex);
					// System.err.println("Method.. " + ps.methodName);
					ps.ex = ExceptionUtils.getStackTrace(ex);
					// TCPChromeDriverUtility.quit("stop");
				}
				return ps;
			} else if (ps.operation == PayloadStruct.OPERATION.INSIGHT) {
				try {
					Insight output = (Insight) ps.payload[0];
					if (output.getREnv() != null) {
						output.setRJavaTranslator(rtMap.get(output.getREnv()));
					}
					ps.payload = new Object[] { "Set insight successfully" };
					ps.payloadClasses = new Class[] { String.class };
					ps.processed = true;
					ps.response = true;
					insightMap.put(output.getInsightId(), output);
				} catch (Exception ex) {
					classLogger.error("Error while processing INSIGHT payload for insight '{}'", ps.insightId, ex);
					// System.err.println("Method.. " + ps.methodName);
					ps.ex = ExceptionUtils.getStackTrace(ex);
					// TCPChromeDriverUtility.quit("stop");
				}
				return ps;
			} else if (ps.operation == PayloadStruct.OPERATION.REACTOR) {
				try {
					Insight insight = insightMap.get(ps.insightId);
					// no need for another thread
					// you are already in a thread
					String reactorName = ps.objId;
					ps.response = true;

					// get the project
					// Project serves no purpose other than just giving me the reactor

					// TODO: on tomcat side, when context changes needs to be told
					// TODO: on tomcat side, when context changes needs to be told
					// TODO: on tomcat side, when context changes needs to be told
					// TODO: on tomcat side, when context changes needs to be told

					// 1) we need to check insight context project
					// 2) then we need to check the project the insight is saved in
					// note for 2 - this can be null

					IReactor reactor = null;
					String contextProjectId = insight.getContextProjectId();
					if (contextProjectId != null) {
						reactor = getProjectReactor(contextProjectId, insight.getContextProjectName(), reactorName);
					}
					if (reactor == null && insight.getProjectId() != null) {
						reactor = getProjectReactor(insight.getProjectId(), insight.getProjectName(), reactorName);
					}
					if (reactor == null) {
						throw new NullPointerException("Could not find reactor with name " + reactorName);
					}
					reactor.setInsight(insight);
					reactor.setNounStore((NounStore) ps.payload[0]);
					classLogger.info("Set the nounstore on reactor");

					// execute
					reactor.In();
					NounMetadata nmd = reactor.execute();
					classLogger.info("Execution of reactor complete");
					// return the response
					ps.payload = new Object[] { nmd };
					ps.payloadClasses = new Class[] { NounMetadata.class };
				} catch (Exception ex) {
					classLogger.error("Error while executing REACTOR operation '{}'", ps.objId, ex);
					// System.err.println("Method.. " + ps.methodName);
					ps.ex = ExceptionUtils.getStackTrace(ex);
					// TCPChromeDriverUtility.quit("stop");
				}
				return ps;
			} else if (ps.operation == PayloadStruct.OPERATION.PROJECT) {
				// make a method call
				try {
					Project project = projectMap.get(ps.projectId);
					if (project == null) {
						project = makeProject(ps.projectId, ps.projectName);
					}

					if (project != null) {
						Method method = findProjectMethod(project, ps.methodName, ps.payloadClasses);
						Object retObject = null;
						retObject = method.invoke(project, ps.payload);
						ps.processed = true;
						ps.response = true;
					}
					ps.payload = new Object[] { "method " + ps.methodName + " execution complete" };
					ps.payloadClasses = new Class[] { String.class };
				} catch (Exception ex) {
					classLogger.error("Error while executing PROJECT operation '{}'", ps.methodName, ex);
					// System.err.println("Method.. " + ps.methodName);
					ps.ex = ExceptionUtils.getStackTrace(ex);
					// TCPChromeDriverUtility.quit("stop");
				}
				return ps;
			}
			// TODO: accounting for chroot in cmd
			// so we might not need to run this anymore across the socket...
//			else if(ps.operation == PayloadStruct.OPERATION.CMD)
//			{
//				// make a method call
//				try {
//					if(ps.methodName.equalsIgnoreCase("constructor")) {
//						String mountName = ""+ ps.payload[0];
//						String dir = "" + ps.payload[1];
//						if(!cmdMap.containsKey(mountName + "__" + dir))
//						{
//							CmdExecUtil cmd = new CmdExecUtil(mountName, dir, null);
//							cmdMap.put(mountName + "__" + dir, cmd);
//						}
//						ps.payload = new Object [] {"constructor execution complete"};
//						ps.payloadClasses = new Class [] {String.class};
//					} else {
//						CmdExecUtil thisCmd = cmdMap.get(ps.insightId);
//						if(thisCmd != null)
//						{
//							String output = thisCmd.executeCommand(""+ps.payload[0]);
//							ps.processed = true;
//							ps.response = true;
//							ps.payload = new Object[] {output};
//						}
//					}
//				} catch(Exception ex) {
//					ps.ex = ExceptionUtils.getStackTrace(ex);						
//					//TCPChromeDriverUtility.quit("stop");
//				}
//
//				return ps;
//			}
		} catch (Exception ex) {
			classLogger.error("Unhandled error while processing payload for method '{}'", ps.methodName, ex);
			ps.ex = ex.getMessage();
		}
		return null;
	}

	/**
	 * Gets a reactor from a project, creating the project reference when needed.
	 * 
	 * @param projectId
	 * @param projectName
	 * @param reactorName
	 * @return resolved reactor, or {@code null} if the project cannot provide it
	 */
	private IReactor getProjectReactor(String projectId, String projectName, String reactorName) {
		Project project = null;
		if (projectMap.containsKey(projectId)) {
			project = projectMap.get(projectId);
		} else {
			project = makeProject(projectId, projectName);
		}
		// I dont know if I can do this or I have to use that jar class loader
		IReactor reactor = project.getReactor(reactorName);
		return reactor;
	}

	/**
	 * Creates and registers a project wrapper for socket-based reactor execution.
	 * 
	 * @param projectId   project id
	 * @param projectName project name
	 * @return initialized project wrapper
	 */
	private Project makeProject(String projectId, String projectName) {
		Project project = new Project();
		project.setProjectId(projectId);
		project.setProjectName(projectName);
		// dont give me a wrapper.. give me the real reactor
		projectMap.put(projectId, project);
		String projectSock = projectId + "__SOCKET";
		DIHelper.getInstance().setProjectProperty(projectSock, project);

		return project;
	}

	/**
	 * Serializes and writes a payload response to the connected client.
	 * 
	 * @param ps payload to write
	 * @return response payload for caller-waited operations, otherwise {@code null}
	 */
	public PayloadStruct writeResponse(PayloadStruct ps) {
		byte[] psBytes = null;
		// if this is the response
		// all set
		// package the bytes and send the response
		if (!ps.response || ps.epoc == null) {
			ps.epoc = "ss" + curEpoc;
			curEpoc++;
			outgoing.put(ps.epoc, ps);
		}

		try {
			psBytes = FstUtil.packBytes(ps);
		} catch (Exception ex) {
			// dont choke this thread
			classLogger.error("Failed to serialize payload response for epoc '{}'", ps.epoc, ex);
			if (psBytes == null) {
				// hmm we are in the non serializable land
				// let us try it this way now
				ps.payload = new String[] { "Output is not serializable. Forcing stringify <" + ps.payload[0] + ">" };
				psBytes = FstUtil.packBytes(ps);
			}
		}

		// send it
		// System.out.println(" Sending bytes " + psBytes.length + " >> " +
		// ps.methodName + " " + ps.epoc + " >> ");
		classLogger.info("Sending {} bytes for method '{}' (epoc '{}')", psBytes.length, ps.methodName, ps.epoc);
		try {
			os.write(psBytes);
			// remove from the epoc queue
		} catch (Exception ex) {
			classLogger.error("Failed to write payload response for epoc '{}'", ps.epoc, ex);
		}

		// if this is what socket is sending
		// i.e. response to an operation core semoss requested
		// job is done - clear it from the queues
		// incoming was the request, outgoing was the response
		if (ps.response) // clear from the current
		{
			// remove from unprocessed
			incoming.remove(ps.epoc);
			outgoing.remove(ps.epoc);
		}
		// if this is a request for core semoss
		// block the thread until we get response
		// notification happens in the run block see below
		// put the current structure into outgoing
		// block on that payload object
		// wait
		else // this is for interim operations
		{
			// put this into unprocessed
			// synchronize on the payload
			// and then wait
			// System.err.println(" Here in request " + ps);
			while (!incoming.containsKey(ps.epoc)) {
				synchronized (ps) {
					try {
						// wait to see if there is response
						classLogger.info("Waiting for response epoc '{}'", ps.epoc);
						ps.wait(averageMillis);
						// once response remove this from the outgoing queue
						// the main input is available on incoming
					} catch (InterruptedException e) {
						Thread.currentThread().interrupt();
						classLogger.error("Interrupted while waiting for response epoc '{}'", ps.epoc, e);
						outgoing.remove(ps.epoc);
						return createErrorResponse(ps,
								"Interrupted while waiting for socket response for epoc '" + ps.epoc + "'");
					}
				}
			}
			classLogger.info("Received response for epoc '{}'", ps.epoc);
			// assumes we already got the response
			outgoing.remove(ps.epoc);
			PayloadStruct responsePayload = incoming.remove(ps.epoc);
			if (responsePayload == null) {
				return createErrorResponse(ps, "Response for epoc '" + ps.epoc + "' was not available");
			}
			return responsePayload;

		}
		return null;
	}

	private PayloadStruct createErrorResponse(PayloadStruct originalRequest, String message) {
		PayloadStruct errorResponse = new PayloadStruct();
		if (originalRequest != null) {
			errorResponse.epoc = originalRequest.epoc;
			errorResponse.operation = originalRequest.operation;
			errorResponse.methodName = originalRequest.methodName;
		}
		errorResponse.response = true;
		errorResponse.processed = true;
		errorResponse.ex = message;
		errorResponse.payload = new Object[] { message };
		errorResponse.payloadClasses = new Class[] { String.class };
		return errorResponse;
	}

	/**
	 * Forces responses for all pending incoming payloads.
	 */
	public void releaseAll() {
		// Snapshot first to avoid iterating over a map being mutated by writeResponse()
		PayloadStruct[] pendingPayloads;
		synchronized (incoming) {
			pendingPayloads = incoming.values().toArray(new PayloadStruct[0]);
		}
		for (PayloadStruct ps : pendingPayloads) {
			if (ps == null) {
				continue;
			}
			String message = "Releasing this payload";
			if (ps.payload != null && ps.payload.length >= 1) {
				message = message + ps.payload[0];
			}
			ps.payload = new String[] { message };
			writeResponse(ps);
		}
	}

	/**
	 * Deletes the socket working folder and shuts down translator/database
	 * resources.
	 */
	public void cleanUp() {
		try {
			if (!test) {
				classLogger.info("Starting shutdown ");
				Iterator<String> envKeys = rtMap.keySet().iterator();
				while (envKeys.hasNext()) {
					String env = envKeys.next();
					AbstractRJavaTranslator rt = rtMap.get(env);
					if (rt != null) {
						rt.endR();
					}
				}

				// we should also close all the dbs that were opened
				String engines = DIHelper.getInstance().getEngineProperty(Constants.ENGINES) + "";
				if (engines != null) {
					String[] engineList = engines.split(";");
					for (int engineIndex = 0; engineIndex < engineList.length; engineIndex++) {
						IDatabaseEngine engine = Utility.getDatabase(engineList[engineIndex]);
						if (engine != null) {
							engine.close();
						}
					}
				}
			}
			// stop the classLogger
			LogManager.shutdown();

			// don't delete output log
			// do it later
			File outFile = new File(mainFolder + "/output.log");
			if (outFile.exists() && outFile.isFile()) {
				outFile.deleteOnExit();
			}

			try {
				FileUtils.deleteDirectory(new File(mainFolder));
			} catch (IOException ignore) {

			}
		} catch (Exception | Error e) {
			// ignore
		}

		// exit out
		System.exit(1);
	}

	/**
	 * Finds an R translator method by name and arguments.
	 * 
	 * @param rt         translator instance
	 * @param methodName method name
	 * @param arguments  argument classes
	 * @return resolved method, or {@code null} if it cannot be found
	 */
	public Method findRMethod(AbstractRJavaTranslator rt, String methodName, Class[] arguments) {
		Method retMethod = null;

		// look for it in the child class if not parent class
		// we can even cache this later
		try {
			if (arguments != null) {
				try {
					retMethod = rt.getClass().getDeclaredMethod(methodName, arguments);
				} catch (Exception ex) {
					// ignore and fallback to superclass lookup
				}
				if (retMethod == null) {
					retMethod = rt.getClass().getSuperclass().getDeclaredMethod(methodName, arguments);
				}
			} else {
				try {
					retMethod = rt.getClass().getDeclaredMethod(methodName);
				} catch (Exception ex) {
					// ignore and fallback to superclass lookup
				}
				if (retMethod == null) {
					retMethod = rt.getClass().getSuperclass().getDeclaredMethod(methodName, arguments);
				}
			}
			classLogger.info("Found method {}", retMethod);
		} catch (NoSuchMethodException e) {
			classLogger.error("Unable to resolve R method '{}'", methodName, e);
		} catch (SecurityException e) {
			classLogger.error("Security manager prevented access to R method '{}'", methodName, e);
		}
		return retMethod;
	}

	/**
	 * Finds a project method by name and arguments.
	 * 
	 * @param rt         project wrapper instance
	 * @param methodName method name
	 * @param arguments  argument classes
	 * @return resolved method, or {@code null} if it cannot be found
	 */
	public Method findProjectMethod(Project rt, String methodName, Class[] arguments) {
		Method retMethod = null;

		// look for it in the child class if not parent class
		// we can even cache this later
		try {
			if (arguments != null) {
				try {
					retMethod = rt.getClass().getDeclaredMethod(methodName, arguments);
				} catch (Exception ex) {
					// ignore and fallback to superclass lookup
				}
				if (retMethod == null) {
					retMethod = rt.getClass().getSuperclass().getDeclaredMethod(methodName, arguments);
				}

			} else {
				try {
					retMethod = rt.getClass().getDeclaredMethod(methodName);
				} catch (Exception ex) {
					// ignore and fallback to superclass lookup
				}
				if (retMethod == null) {
					retMethod = rt.getClass().getSuperclass().getDeclaredMethod(methodName, arguments);
				}
			}
			classLogger.info("Found method {}", retMethod);
		} catch (NoSuchMethodException e) {
			classLogger.error("Unable to resolve project method '{}'", methodName, e);
		} catch (SecurityException e) {
			classLogger.error("Security manager prevented access to project method '{}'", methodName, e);
		}
		return retMethod;
	}

	/**
	 * Finds a chrome utility method by name and arguments.
	 * 
	 * @param methodName method name
	 * @param arguments  argument classes
	 * @return resolved method, or {@code null} if it cannot be found
	 */
	public Method findChromeMethod(String methodName, Class[] arguments) {
		Method retMethod = null;

		try {
			if (arguments != null) {
				try {
					retMethod = TCPChromeDriverUtility.class.getDeclaredMethod(methodName, arguments);
				} catch (Exception ex) {
					// ignore and continue
				}
			} else {
				try {
					retMethod = TCPChromeDriverUtility.class.getDeclaredMethod(methodName);
				} catch (Exception ex) {
					// ignore and continue
				}
			}
			classLogger.info("Found method {}", retMethod);
		} catch (SecurityException e) {
			classLogger.error("Security manager prevented access to chrome method '{}'", methodName, e);
		}
		return retMethod;
	}

	/**
	 * Invokes a method on the R translator and unwraps invocation target
	 * exceptions.
	 * 
	 * @param rt2       translator instance
	 * @param method    method to invoke
	 * @param arguments invocation arguments
	 * @return invocation return value
	 * @throws Exception when the underlying translator method throws
	 */
	public Object runMethodR(AbstractRJavaTranslator rt2, Method method, Object[] arguments) throws Exception {
		try {
			Object retObject = null;
			retObject = method.invoke(rt2, arguments);
			return retObject;
		} catch (InvocationTargetException e) {
			throw (Exception) e.getCause();
		}
	}

	/**
	 * Invokes a static chrome utility method.
	 * 
	 * @param method    method to invoke
	 * @param arguments invocation arguments
	 * @return invocation return value
	 * @throws Exception when reflection invocation fails
	 */
	public Object runMethodChrome(Method method, Object[] arguments) throws Exception {
		Object retObject = null;

		retObject = method.invoke(TCPChromeDriverUtility.class, arguments);

		return retObject;
	}

	/**
	 * Retrieves or creates an R translator for the requested environment.
	 * 
	 * @param env environment id
	 * @return translator bound to the provided environment
	 */
	private AbstractRJavaTranslator getTranslator(String env) {
		AbstractRJavaTranslator translator = rtMap.get(env);
		if (translator != null) {
			return translator;
		}

		synchronized (translatorInitLock) {
			translator = rtMap.get(env);
			if (translator != null) {
				return translator;
			}

			boolean useJri = DIHelper.getInstance().getProperty(Constants.R_CONNECTION_JRI) == null
					|| DIHelper.getInstance().getProperty(Constants.R_CONNECTION_JRI).equalsIgnoreCase("true");
			if (useJri) {
				translator = new RJavaJriTranslator();
				translator.setLogger(classLogger);
				translator.startR();
				translator.initREnv(env);
			} else {
				RJavaRserveTranslator rserveTranslator = new RJavaRserveTranslator();
				rserveTranslator.setLogger(classLogger);
				RConnection existingConnection = this.retCon;
				if (existingConnection == null) {
					rserveTranslator.startR();
					this.retCon = rserveTranslator.getConnection();
				} else {
					rserveTranslator.setConnection(existingConnection);
					rserveTranslator.initREnv(env);
				}
				translator = rserveTranslator;
			}
			rtMap.put(env, translator);
			return translator;
		}
	}

	/**
	 * Sets the socket output stream.
	 * 
	 * @param os output stream
	 */
	public void setOutputStream(OutputStream os) {
		this.os = os;
	}

	/**
	 * Sets the socket input stream.
	 * 
	 * @param is input stream
	 */
	public void setInputStream(InputStream is) {
		this.is = is;
	}

	/**
	 * Sets the shared server socket reference.
	 * 
	 * @param socket server socket
	 */
	public void setServerSocket(ServerSocket socket) {
		this.socket = socket;
	}

	/**
	 * Reads payload bytes from the socket stream and dispatches requests/responses
	 * until the handler stops.
	 */
	@Override
	public void run() {
		// there are 2 types of interactions
		// #1 SEMOSS Core sends a request and this responds - In this case the packet
		// comes with response = false to say this is a request
		// #2 This asks SEMOSS core to perform an operation like database insert or
		// update etc.

		while (!done) {
			try {
				int bytesToRead = offset;
				if (lenBytes != null && lenBytesReadSoFar == lenBytes.length) // only get in here if you have read
																				// everything
				{
					bytesToRead = ByteBuffer.wrap(lenBytes).getInt();
					if (curBytes == null) {
						curBytes = new byte[bytesToRead]; // block it
					}

					int bytesRead = is.read(curBytes, bytesReadSoFar, (curBytes.length - bytesReadSoFar)); // block
					bytesReadSoFar = bytesReadSoFar + bytesRead;

					if (bytesReadSoFar == curBytes.length) // we got what we need.. let us go
					{
						Object retObject = FstUtil.deserialize(curBytes);

						// need something here which basically tries to see if this is a request or a
						// response
						// #1 - This is a request for socket - handle it
						if (!((PayloadStruct) retObject).response) // this is a request that is coming here
						{
							lenBytes = null;
							curBytes = null;
							bytesReadSoFar = 0;
							lenBytesReadSoFar = 0;

							if (blocking) {
								PayloadStruct output = getFinalOutput((PayloadStruct) retObject);
								writeResponse(output);

							}

							else {
								WorkerThread wt = new WorkerThread(this, (PayloadStruct) retObject);
								Thread t = new Thread(wt);
								t.start();
							}
						}
						// #2 - Response to an operation being performed by core semoss
						else {
							// this is a response to the request that just came in
							// synchronize on the ps and then notify
							PayloadStruct responseStruct = (PayloadStruct) retObject;
							classLogger.info("Received payload with epoc '{}'", responseStruct.epoc);
							PayloadStruct requestStruct = outgoing.get(responseStruct.epoc);
							classLogger.info("Outgoing queue contains response epoc '{}': {}", responseStruct.epoc,
									outgoing.containsKey(responseStruct.epoc));
							incoming.put(responseStruct.epoc, responseStruct);
							if (requestStruct != null) {
								synchronized (requestStruct) {
									requestStruct.notifyAll(); // this will give the thread back what it was looking for
								}
							}
							lenBytes = null;
							curBytes = null;
							bytesReadSoFar = 0;
							lenBytesReadSoFar = 0;
						}
					}
				} else {
					if (lenBytes == null) {
						lenBytes = new byte[bytesToRead]; // block it
					}
					int bytesRead = is.read(lenBytes, lenBytesReadSoFar, (lenBytes.length - lenBytesReadSoFar)); // block
					lenBytesReadSoFar = lenBytesReadSoFar + bytesRead;
				}
			} catch (IOException e) {
				classLogger.error("Socket handler read failed; signaling crash recovery", e);
				try {
					// Notify SocketServer.run() so it can unwind/cleanup and re-listen.
					this.done = true;
					server.signalCrash();
				} catch (Exception e1) {
					classLogger.error("Failed to signal server crash recovery", e1);
				}
				// dont quit.. work hard
				if (!SocketServer.isMulti()) {
					System.exit(1);
				}
			}
		}
	}

	/**
	 * Gets the stored payload for a given epoc.
	 * 
	 * @param epoc epoc identifier
	 * @return payload associated with the epoc, or {@code null}
	 */
	public PayloadStruct getPayloadForEpoc(String epoc) {
		return incoming.get(epoc);
	}

}
