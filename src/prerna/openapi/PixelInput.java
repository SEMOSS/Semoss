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
package prerna.openapi;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Repeatable;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Refines what is inferred for one of a reactor's inputs. Every input in
 * {@code keysToGet} is published without it; use it where the inferred type,
 * description or requiredness is not good enough.
 *
 * <pre>{@code
 * @PixelInput(name = "value", type = Object.class)
 * @PixelInput(name = "limit", type = Integer.class, required = Requirement.OPTIONAL)
 * public class EchoReactor extends AbstractReactor {
 * }</pre>
 *
 * Naming a key the reactor does not declare fails the OpenAPI export.
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
@Repeatable(PixelInputs.class)
public @interface PixelInput {

	/** Whether an input must be passed. */
	enum Requirement {
		/** As {@code keyRequired} says. */
		INFERRED,
		/** It must be passed. */
		REQUIRED,
		/** It may be left out. */
		OPTIONAL
	}

	/** The input's key, as in {@code keysToGet} (the canonical name, before any alias). */
	String name();

	/**
	 * Its type: {@code String}, {@code Integer}, {@code Long}, {@code Double},
	 * {@code Boolean}, an enum, {@code Map}, {@code List} (with {@link #items()}),
	 * or {@code Object} for any value. {@code Void}, the default, keeps the
	 * type inferred from {@code getKeyTypeForMCP}.
	 */
	Class<?> type() default Void.class;

	/** The element type when {@link #type()} is {@code List}. */
	Class<?> items() default Void.class;

	/** Its description. Empty, the default, keeps {@code getDescriptionForKey}. */
	String description() default "";

	/** Whether it must be passed. {@code INFERRED}, the default, follows {@code keyRequired}. */
	Requirement required() default Requirement.INFERRED;
}
