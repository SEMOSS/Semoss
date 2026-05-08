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
package prerna.reactor.utils;

import java.io.PrintWriter;
import java.io.StringWriter;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.Gson;

import prerna.om.Insight;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.tcp.PayloadStruct;
import prerna.tcp.client.workers.NativePyEngineWorker;
import prerna.util.Constants;

public class RemoteEngineRunReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(RemoteEngineRunReactor.class);
	
	public RemoteEngineRunReactor() {
		this.keysToGet = new String [] {ReactorKeysEnum.PAYLOAD.getKey()}; // just get this as string and turn it into json
		this.keyRequired = new int [] {1};
	}
	
	@Override
	public NounMetadata execute() {
		this.organizeKeys();
		
		// get the PayloadStruct string
		String message = keyValue.get(ReactorKeysEnum.PAYLOAD.getKey());
		PayloadStruct ps = new Gson().fromJson(message, PayloadStruct.class);
		ps = convertPayloadClasses(ps);
		
		// run the engine call
		NativePyEngineWorker pyw = new NativePyEngineWorker(this.insight.getUser(), ps, this.insight);
		pyw.run();

		// retrieve the output
		PayloadStruct output = null;
		try {
			output = pyw.getOutput();
			output.payloadClasses = null;
		} catch (Exception ex) {
			StringWriter sw = new StringWriter();
			PrintWriter pw = new PrintWriter(sw);
			ex.printStackTrace(pw);
			output.response = true;
			output.ex = sw.toString();
		}
		
		return new NounMetadata(output, PixelDataType.CUSTOM_DATA_STRUCTURE);
	}
	
    private PayloadStruct convertPayloadClasses(PayloadStruct input) {
    	if(input.payloadClassNames != null) {
    		input.payloadClasses = new Class[input.payloadClassNames.length];
    		for(int classIndex = 0;classIndex < input.payloadClassNames.length;classIndex++) {
    			try {
					String className = input.payloadClassNames[classIndex];
					input.payloadClasses[classIndex] = Class.forName(className);
					if(input.payloadClasses[classIndex] == Insight.class)
					{
						Insight insight = this.insight;
						input.payload[classIndex] = insight;
					}
				} catch (ClassNotFoundException e) {
					classLogger.error(Constants.STACKTRACE, e);
				}
    		}
    	}
    	
    	return input;
    }
}
