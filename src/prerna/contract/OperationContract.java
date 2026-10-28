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
package prerna.contract;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Immutable operation metadata shared by documentation and future client generators. */
public record OperationContract(String source, Kind kind, String description, Policy policy,
		DeprecationInfo deprecation, List<Input> inputs, List<Output> outputs,
		InvocationInfo invocation, RestInfo rest, Map<String, TypeContract.ObjectType> definitions,
		List<Diagnostic> diagnostics, String name, List<String> aliases) {
	public OperationContract {
		inputs = List.copyOf(inputs);
		outputs = List.copyOf(outputs);
		definitions = Collections.unmodifiableMap(new LinkedHashMap<>(definitions));
		diagnostics = List.copyOf(diagnostics);
		aliases = List.copyOf(aliases);
	}
	public OperationContract(String source, Kind kind, String description, Policy policy,
			DeprecationInfo deprecation, List<Input> inputs, List<Output> outputs,
			InvocationInfo invocation, RestInfo rest, Map<String, TypeContract.ObjectType> definitions,
			List<Diagnostic> diagnostics) {
		this(source, kind, description, policy, deprecation, inputs, outputs, invocation, rest,
				definitions, diagnostics, "", List.of());
	}

	public enum Kind { REST, PIXEL }
	public enum Requirement { REQUIRED, OPTIONAL, UNKNOWN }
	public enum Severity { WARNING, ERROR }
	public record Diagnostic(Severity severity, String source, String message) {}
	public record Policy(List<String> ignoreReasons, List<String> internalReasons) {
		public Policy {
			ignoreReasons = List.copyOf(ignoreReasons);
			internalReasons = List.copyOf(internalReasons);
		}
		public boolean ignored() { return !ignoreReasons.isEmpty(); }
		public boolean internal() { return !internalReasons.isEmpty(); }
	}
	public record DeprecationInfo(boolean deprecated, String since, boolean forRemoval,
			String reason, String replacement) {}
	public record Input(String name, TypeContract.TypeUse type, String description,
			Requirement requirement, List<String> aliases, Parameter.Binding binding,
			Parameter.Encoding encoding, String defaultValue) {
		public Input { aliases = List.copyOf(aliases); }
	}
	/** status -1 denotes an unspecified HTTP status, or a Pixel result. */
	public record Output(TypeContract.TypeUse type, int status, String mediaType, String description, String name) {
		public Output(TypeContract.TypeUse type, int status, String mediaType, String description) {
			this(type, status, mediaType, description, "");
		}
	}
	public record InvocationInfo(Invocation.Mode mode, List<String> order, String variadic,
			boolean canOmitParameters) {
		public InvocationInfo { order = List.copyOf(order); }
	}
	/** Path is relative to the JAX-RS application; servlet discovery is separate. */
	public record RestInfo(String method, String path, List<String> consumes, List<String> produces) {
		public RestInfo {
			consumes = List.copyOf(consumes);
			produces = List.copyOf(produces);
		}
	}
}
