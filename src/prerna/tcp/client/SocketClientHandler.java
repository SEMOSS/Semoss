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

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.tcp.PayloadStruct;
import prerna.tcp.client.workers.NativePyEngineWorker;
import prerna.util.FstUtil;

public class SocketClientHandler implements Runnable {

	private static final Logger classLogger = LogManager.getLogger(SocketClientHandler.class);

	private int offset = 4;

	private boolean done = false;

	private byte[] lenBytes = null;
	private int lenBytesReadSoFar = 0;
	private byte[] curBytes = null;
	private int bytesReadSoFar = 0;

	private SocketClient socketClient = null;
	private InputStream in = null;

	public void setClient(SocketClient socketClient) {
		this.socketClient = socketClient;
	}

	public void setInputStream(InputStream in) {
		this.in = in;
	}

	public void handleResponse(Object obj) {
		PayloadStruct ps = (PayloadStruct) obj;
		try {
			if (ps != null) {
				if (ps.ex != null) {
					classLogger.warn("Payload for epoc {} came with an exception: {}", ps.epoc, ps.ex);
				}
				String id = ps.epoc;

				// hand the response to the waiting caller and wake it
				PayloadStruct lock = socketClient.requestMap.remove(id);
				socketClient.responseMap.put(id, ps);
				if (lock != null) {
					lock.signalResponse();
				}
			}
		} catch (Exception ex) {
			classLogger.error("Error handling incoming payload response for epoc: {}", ps != null ? ps.epoc : null, ex);
		}
	}

	@Override
	public void run() {
		while (!done) {
			try {
				int bytesToRead = offset;
				int readBytes = 0;
				if (lenBytes != null) {
					bytesToRead = ByteBuffer.wrap(lenBytes).getInt();
					if (curBytes == null) {
						curBytes = new byte[bytesToRead];
					}

					readBytes = in.read(curBytes, bytesReadSoFar, (curBytes.length - bytesReadSoFar)); // blocking read
					bytesReadSoFar = bytesReadSoFar + readBytes;

					if (bytesReadSoFar == curBytes.length && readBytes != -1) {
						try {
							PayloadStruct ps = (PayloadStruct) FstUtil.deserialize(curBytes);
							if (ps != null) {
								classLogger.info("Received payload {} of {} bytes", ps.epoc, curBytes.length);
								if (ps.response) {
									handleResponse(ps);
									lenBytes = null;
									bytesReadSoFar = 0;
									lenBytesReadSoFar = 0;
									curBytes = null;
								} else if (ps.operation == PayloadStruct.OPERATION.ENGINE) {
									// a reverse request from the server - run it and send the result back
									NativePyEngineWorker ew = new NativePyEngineWorker(socketClient.getUser(), ps);
									ew.run();
									socketClient.executeCommand(ew.getOutput());

									lenBytes = null;
									bytesReadSoFar = 0;
									lenBytesReadSoFar = 0;
									curBytes = null;
								}
							} else {
								classLogger.warn("Failed to deserialize payload of {} bytes (bytes read {})",
										curBytes.length, readBytes);
								lenBytes = null;
								bytesReadSoFar = 0;
								lenBytesReadSoFar = 0;
								curBytes = null;
							}
						} catch (Exception ex) {
							// bad packet - reset the buffers and continue so the reader thread
							// recovers on the next message instead of hanging
							int badPacketLength = curBytes.length;
							lenBytes = null;
							bytesReadSoFar = 0;
							lenBytesReadSoFar = 0;
							curBytes = null;
							classLogger.error("Failed to deserialize payload of {} bytes (bytes read {})",
									badPacketLength, readBytes, ex);
						}
					}
				} else {
					if (lenBytes == null) {
						lenBytes = new byte[bytesToRead];
					}
					int bytesRead = in.read(lenBytes, lenBytesReadSoFar, (lenBytes.length - lenBytesReadSoFar)); // blocking
																													// read
					lenBytesReadSoFar = lenBytesReadSoFar + bytesRead;
				}

				if (readBytes < 0) // stream is closed - kill this thread
				{
					done = true;
					this.socketClient.crash();
				}
			} catch (IOException e) {
				classLogger.error("IO error reading from socket; crashing the client", e);
				done = true;
				this.socketClient.crash();
			}
		}
	}
}
