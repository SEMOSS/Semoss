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
package prerna.reactor.test;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.engine.api.IHeadersDataRow;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.om.task.BasicIteratorTask;
import prerna.util.ArrayUtilityMethods;
import prerna.util.Constants;
import prerna.util.Utility;

public class RunAliasMatchReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(RunAliasMatchReactor.class);

	private String aliasHeader = "Alias_1";
	private String hashCodeHeader = "Hashcode";

	@Override
	public NounMetadata execute() {
		Iterator<IHeadersDataRow> inputIterator = null;
		try {
			inputIterator = getInputIterator();
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
		}
		Iterator<IHeadersDataRow> proposalIterator = null;
		try {
			proposalIterator = getProposalIterator();
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
		}

		// need to check if all aliases and all hashcodes are the same
		Map<String, String> inputHash = new HashMap<>();
		Map<String, String> proposalHash = new HashMap<>();

		if (inputIterator != null) {
			while (inputIterator.hasNext()) {
				IHeadersDataRow nextData = inputIterator.next();
				String[] headers = nextData.getHeaders();
				Object[] values = nextData.getValues();
				int aliasIndex = ArrayUtilityMethods.arrayContainsValueAtIndexIgnoreCase(headers, aliasHeader);
				int hashIndex = ArrayUtilityMethods.arrayContainsValueAtIndexIgnoreCase(headers, hashCodeHeader);
				inputHash.put(values[aliasIndex].toString(), values[hashIndex].toString());
			}
		}

		if (proposalIterator != null) {
			while (proposalIterator.hasNext()) {
				IHeadersDataRow nextData = proposalIterator.next();
				String[] headers = nextData.getHeaders();
				Object[] values = nextData.getValues();
				int aliasIndex = ArrayUtilityMethods.arrayContainsValueAtIndexIgnoreCase(headers, aliasHeader);
				int hashIndex = ArrayUtilityMethods.arrayContainsValueAtIndexIgnoreCase(headers, hashCodeHeader);
				proposalHash.put(values[aliasIndex].toString(), values[hashIndex].toString());
			}
		}

		int count = 0;
		for (String proposalKey : proposalHash.keySet()) {
			if (inputHash.containsKey(proposalKey)) {
				String proposalHashValue = proposalHash.get(proposalKey);
				String inputHashValue = inputHash.get(proposalKey);
				if (!proposalHashValue.equals(inputHashValue)) {
					count++;
					classLogger.info(Utility.cleanLogString(proposalKey));
					classLogger.info("input: " + inputHashValue);
					classLogger.info("proposal: " + proposalHashValue);
					classLogger.info("________________________");
				}
			}
		}

		classLogger.info("TOTAL NOT MATCHING: " + count);
		return null;
	}

	private Iterator<IHeadersDataRow> getInputIterator() throws Exception {
		GenRowStruct allNouns = getNounStore().getGenRowStruct("INPUT");
		Iterator<IHeadersDataRow> iterator = null;

		if (allNouns != null) {
			BasicIteratorTask task = (BasicIteratorTask) allNouns.get(0);
			iterator = task.getIterator();
		}
		return iterator;
	}

	private Iterator<IHeadersDataRow> getProposalIterator() throws Exception {
		GenRowStruct allNouns = getNounStore().getGenRowStruct("PROPOSALS");
		Iterator<IHeadersDataRow> iterator = null;

		if (allNouns != null) {
			BasicIteratorTask task = (BasicIteratorTask) allNouns.get(0);
			iterator = task.getIterator();
		}
		return iterator;
	}
}
