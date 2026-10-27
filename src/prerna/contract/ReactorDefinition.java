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

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/** Declarative reactor metadata used both by AbstractReactor2 and the contract reader. */
public record ReactorDefinition(String description, List<Argument> arguments, Result result,
		OperationContract.InvocationInfo invocation, OperationContract.Policy policy,
		OperationContract.DeprecationInfo deprecation) {
	public ReactorDefinition {
		Objects.requireNonNull(description);
		arguments = List.copyOf(arguments);
		Objects.requireNonNull(result);
		Objects.requireNonNull(invocation);
		Objects.requireNonNull(policy);
		Objects.requireNonNull(deprecation);
	}

	/** Must have a public no-argument constructor and perform no runtime work. */
	public interface Provider { ReactorDefinition definition(); }

	public record Argument(String name, Class<?> type, boolean required, String description,
			List<String> aliases, boolean nullable) {
		public Argument {
			Objects.requireNonNull(name);
			Objects.requireNonNull(type);
			Objects.requireNonNull(description);
			aliases = List.copyOf(aliases);
		}
		public Argument(String name, Class<?> type, boolean required, String description) {
			this(name, type, required, description, List.of(), false);
		}
	}
	public record Result(Class<?> type, String description, boolean nullable) {
		public Result { Objects.requireNonNull(type); Objects.requireNonNull(description); }
	}

	public static Builder builder(String description) { return new Builder(description); }

	/** Builds immutable definitions; ordering is explicit for positional inputs. */
	public static final class Builder {
		private final String description;
		private final List<Argument> arguments = new ArrayList<>();
		private Result result = new Result(Object.class, "Unspecified output", false);
		private OperationContract.InvocationInfo invocation = new OperationContract.InvocationInfo(
				Invocation.Mode.NAMED, List.of(), "", false);
		private final List<String> ignored = new ArrayList<>();
		private final List<String> internal = new ArrayList<>();
		private OperationContract.DeprecationInfo deprecation = new OperationContract.DeprecationInfo(false, "", false, "", "");

		private Builder(String description) { this.description = description; }
		public Builder parameter(Argument argument) { arguments.add(argument); return this; }
		public Builder returns(Class<?> type, String description) {
			return returns(new Result(type, description, false));
		}
		public Builder returns(Result result) { this.result = result; return this; }
		public Builder invocation(Invocation.Mode mode, List<String> order, String variadic, boolean canOmitParameters) {
			invocation = new OperationContract.InvocationInfo(mode, order, variadic, canOmitParameters);
			return this;
		}
		public Builder ignore(String reason) { ignored.add(reason); return this; }
		public Builder internal(String reason) { internal.add(reason); return this; }
		public Builder deprecated(String since, boolean forRemoval, String reason, String replacement) {
			deprecation = new OperationContract.DeprecationInfo(true, since, forRemoval, reason, replacement);
			return this;
		}
		public ReactorDefinition build() {
			return new ReactorDefinition(description, arguments, result, invocation,
					new OperationContract.Policy(ignored, internal), deprecation);
		}
	}
}
