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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.reactor.AbstractReactor;
import prerna.reactor.IReactor;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.tcp.PayloadStruct;

public class CustomReactorWrapper extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(CustomReactorWrapper.class);

	// this takes the custom reactor
	// sets up to go across socket
	// returns the result
	public IReactor realReactor = null;
	public String reactorCallName = null;
	SocketClient sc = null;

	@Override
	public NounMetadata execute() {
		sc = this.insight.getUser().getPythonSocketClient(true);

		InsightSerializer is = new InsightSerializer(this.insight);
		is.serializeInsight(false);

		PayloadStruct ps = new PayloadStruct();
		ps.operation = ps.operation.REACTOR;

		// set everything from the noun store
		// hopefully this serializes well
		ps.payload = new Object[] { this.store };
		ps.payloadClasses = new Class[] { this.store.getClass() };
		ps.objId = reactorCallName;
		ps.insightId = this.insight.getInsightId();

		PayloadStruct retStruct = (PayloadStruct) sc.executeCommand(ps);
		classLogger.info("Got the response for reactor " + ps.payload[0]);

		// did we have an error?
		if (retStruct.ex != null) {
			return NounMetadata.getErrorNounMessage(retStruct.ex);
		}
		return (NounMetadata) retStruct.payload[0];
	}
}
