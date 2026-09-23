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
package prerna.sablecc2.comm;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Marker;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.message.Message;
import org.apache.logging.log4j.message.SimpleMessage;

import prerna.util.Utility;

public class InMemoryConsole extends Logger {

	private String jobID;
	private boolean partial;

	public InMemoryConsole(String name, String jobId) {
		super((LoggerContext) LogManager.getContext(false), name, null);
		this.jobID = jobId;
		setLevel(Level.INFO);
	}

	public void setPartial(boolean partial) {
		this.partial = partial;
	}

	public void setJobID(String jobID) {
		this.jobID = jobID;
	}

	/**
	 * Single interception point for all log output. Every public logging call on a
	 * log4j2 core Logger - regardless of level, argument count, or whether it uses
	 * parameterized ("{}") placeholders - is converted to a {@link Message} and
	 * funneled through this method before being handed to the appenders. Overriding
	 * it here (rather than the individual per-level, per-arity public methods)
	 * guarantees that every log call, however it is written, is formatted and
	 * mirrored to the in-memory job console.
	 */
	@Override
	protected void log(Level level, Marker marker, String fqcn, StackTraceElement location, Message message,
			Throwable throwable) {
		// resolve "{}" placeholders against their arguments once, here
		String formatted = message == null ? "" : message.getFormattedMessage();

		// route to the normal appenders with a sanitized message (log injection guard),
		// preserving the caller location and any throwable for the stack trace.
		// the local is typed as Message so the super call resolves unambiguously
		Message cleaned = new SimpleMessage(Utility.cleanLogString(formatted));
		super.log(level, marker, fqcn, location, cleaned, throwable);

		// stream the formatted text to the in-memory job console
		addToJobOutput(level, formatted);
	}

	/**
	 * Push the formatted message to the job's output stream. INFO goes to standard
	 * out (partial or full); everything else (WARN/ERROR/FATAL/DEBUG/TRACE) goes to
	 * standard error.
	 */
	private void addToJobOutput(Level level, String message) {
		if (Level.INFO.equals(level)) {
			if (partial) {
				PixelJobManager.getManager().addPartialOut(jobID, message);
			} else {
				PixelJobManager.getManager().addStdOut(jobID, message);
			}
		} else {
			PixelJobManager.getManager().addStdErr(jobID, message);
		}
	}
}
