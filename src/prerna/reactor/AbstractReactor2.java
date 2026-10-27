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
package prerna.reactor;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.json.JSONObject;

import prerna.contract.ContractReader;
import prerna.contract.ContractSchemas;
import prerna.contract.OperationContract;

/**
 * Opt-in reactor base backed by contract annotations on the concrete reactor.
 * Declare Description, Parameter, Returns, and Invocation alongside execute().
 * A ReactorMetadata provider remains supported for reusable definitions.
 * Input lookup and required-key enforcement remain in AbstractReactor; this class
 * does not add runtime value validation or change execution/authorization.
 */
public abstract class AbstractReactor2 extends AbstractReactor {
	private final OperationContract contract;

	protected AbstractReactor2() {
		contract = ContractReader.read(getClass());
		if (contract.invocation() == null) {
			throw new IllegalArgumentException(getClass().getSimpleName()
					+ " must declare @Invocation with its supported argument style and positional order."
					+ " A @ReactorMetadata provider may also supply the invocation definition.");
		}
		Map<String, OperationContract.Input> inputs = contract.inputs().stream()
				.collect(Collectors.toMap(OperationContract.Input::name, Function.identity()));
		List<String> order = contract.invocation().order().isEmpty()
				? contract.inputs().stream().map(OperationContract.Input::name).toList() : contract.invocation().order();
		keysToGet = order.stream().map(name -> {
			List<String> aliases = inputs.get(name).aliases();
			return name + (aliases.isEmpty() ? "" : "," + String.join(",", aliases));
		}).toArray(String[]::new);
		keyRequired = order.stream().mapToInt(name -> inputs.get(name).requirement() == OperationContract.Requirement.REQUIRED ? 1 : 0).toArray();
	}

	/** Same immutable definition exposed by the offline contract reader. */
	public final OperationContract getContract() { return contract; }

	@Override
	public final String getReactorDescription() { return contract.description(); }

	@Override
	protected final String getDescriptionForKey(String key) {
		return contract.inputs().stream().filter(input -> input.name().equalsIgnoreCase(key)
				|| input.aliases().stream().anyMatch(alias -> alias.equalsIgnoreCase(key)))
				.map(OperationContract.Input::description).findFirst().orElseGet(() -> super.getDescriptionForKey(key));
	}

	@Override
	public final JSONObject getResponseSchema() { return ContractSchemas.output(contract); }

	@Override
	public final JSONObject getMcpProperties() { return ContractSchemas.inputs(contract).getJSONObject("properties"); }

	@Override
	public final JSONObject asMcpTool() {
		JSONObject tool = super.asMcpTool();
		tool.put("inputSchema", ContractSchemas.inputs(contract).put("title", tool.getString("name") + "_Arguments"));
		return tool;
	}
}
