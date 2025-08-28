/***************************************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
 *
 * If your use of this software does not include any GPLv2 components: Licensed under the Apache
 * License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 ***************************************************************************************************/
package prerna.tcp.client.workers;

import java.io.IOException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONArray;
import prerna.auth.User;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.engine.api.IDatabaseEngine;
import prerna.engine.api.IEngine;
import prerna.engine.api.IRawSelectWrapper;
import prerna.om.IStringExportProcessor;
import prerna.om.Insight;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.tcp.PayloadStruct;
import prerna.util.Constants;
import prerna.util.Utility;

public class NativePyEngineWorker implements Runnable {

	private static final Logger classLogger = LogManager.getLogger(NativePyEngineWorker.class);

	/*
	 * NOTE This class has been repurposed and is only used for database execution
	 * All other operations should be send via a reactor call instead of directly
	 * invoking the database
	 */

	private PayloadStruct ps = null;
	private PayloadStruct output = null;
	private User user = null;
	private Insight insight = null;

	public NativePyEngineWorker(User user, PayloadStruct ps) {
		this.ps = ps;
		this.user = user;
	}

	public NativePyEngineWorker(User user, PayloadStruct ps, Insight insight) {
		this.ps = ps;
		this.user = user;
		this.insight = insight;
	}

	@Override
	public void run() {
		IRawSelectWrapper wrapper = null;
		try {
			String engineId = ps.objId;
			boolean canAccess = SecurityEngineUtils.userCanViewEngine(user, engineId);
			if (canAccess) {
				if (ps.engineType.equalsIgnoreCase("DATABASE") && ps.methodName.equalsIgnoreCase("execquery")) {
					IEngine engine = Utility.getDatabase(engineId);
					wrapper = WrapperManager.getInstance().getRawWrapper((IDatabaseEngine) engine, ps.payload[0] + "");
					// do the logic of converting it into
					String fileLocation = null;
					if (this.insight == null || (ps.payload.length > 1 && ps.payload[1].equals("json"))) {
						JSONArray jsonPayload = Utility.writeResultToJsonObject(wrapper, null,
								new IStringExportProcessor() {
									// we need to replace all inner quotes with ""
									@Override
									public String processString(String input) {
										return input.replace("\"", "\\\"");
									}
								});
						ps.payload = new Object[]{jsonPayload};
					} else {
						fileLocation = insight.getInsightFolder() + "/a_" + Utility.getRandomString(5) + ".json";
						Utility.writeResultToJson(fileLocation, wrapper, null, new IStringExportProcessor() {
							// we need to replace all inner quotes with ""
							@Override
							public String processString(String input) {
								return input.replace("\"", "\\\"");
							}
						});
						fileLocation = fileLocation.replace("\\", "/");
						ps.payload = new Object[]{fileLocation};
					}
				} else {
					throw new IllegalArgumentException("Can only execute a database query using this method");
				}
			} else {
				throw new IllegalArgumentException(
						"Engine " + engineId + " does not exist or user does not have access to it");
			}
			// got the response
			ps.response = true;
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			// Get the message from the current exception
			String errorMessage = (e.getCause() != null) ? e.getCause().getMessage() : e.getLocalizedMessage();
			// if its null, pass a generic message
			if (errorMessage == null) {
				errorMessage = "Runtime Error Processing Python Command";
			}
			ps.ex = errorMessage;
			ps.response = true;
		} finally {
			if (wrapper != null) {
				try {
					wrapper.close();
				} catch (IOException e) {
					classLogger.error(Constants.STACKTRACE, e);
				}
			}
		}
		output = ps;
	}

	/**
	 * @return
	 */
	public PayloadStruct getOutput() {
		return this.output;
	}
}
