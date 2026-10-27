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

/** A language-neutral wire type graph. References resolve against definitions. */
public record TypeContract(TypeUse root, Map<String, ObjectType> definitions,
		List<OperationContract.Diagnostic> diagnostics) {
	public TypeContract {
		definitions = Collections.unmodifiableMap(new LinkedHashMap<>(definitions));
		diagnostics = List.copyOf(diagnostics);
	}

	/** Certainty is separate from the known outer shape and its nested fields. */
	public enum Knowledge { VERIFIED, DYNAMIC, UNVERIFIED }
	public enum ScalarKind { STRING, BOOLEAN, INTEGER, DECIMAL }

	/** Metadata on one use of a type, including a collection's element type. */
	public record TypeUse(Shape shape, boolean nullable, String description, String format,
			Knowledge knowledge, String reason) {
		public TypeUse withNullable(boolean value) {
			return new TypeUse(shape, value, description, format, knowledge, reason);
		}
	}

	public sealed interface Shape permits Scalar, Sequence, Dictionary, Union, Reference, Unknown, Empty {}
	public record Scalar(ScalarKind kind, List<String> values) implements Shape {
		public Scalar { values = List.copyOf(values); }
	}
	public record Sequence(TypeUse element) implements Shape {}
	/** JSON object keys are strings. */
	public record Dictionary(TypeUse value) implements Shape {}
	public record Union(List<TypeUse> alternatives) implements Shape {
		public Union { alternatives = List.copyOf(alternatives); }
	}
	public record Reference(String id) implements Shape {}
	public record Unknown() implements Shape {}
	/** An absent response body, distinct from a JSON null. */
	public record Empty() implements Shape {}
	public record Property(String name, TypeUse type, boolean required) {}
	public record ObjectType(String id, List<Property> properties) {
		public ObjectType { properties = List.copyOf(properties); }
	}
}
