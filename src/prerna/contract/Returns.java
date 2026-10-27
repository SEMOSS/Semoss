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

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.annotation.Repeatable;

/** A serialized result, not the Java Response or NounMetadata envelope. */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.TYPE, ElementType.METHOD})
@Repeatable(Returns.List.class)
public @interface Returns {
	/** A wire type, or Object.class when declaring an object with fields(). */
	Class<?> value() default Object.class;
	/** Inline object fields; use these or a concrete value type, not both. */
	Field[] fields() default {};
	/** -1 means no explicit HTTP status; required for Pixel outputs. */
	int status() default -1;
	/** Shorthand when multiple HTTP statuses share the same body and description. */
	int[] statuses() default {};
	String mediaType() default "";
	String description() default "";
	boolean nullable() default false;

	/** One serialized field in a small inline object contract. */
	@Documented
	@Retention(RetentionPolicy.RUNTIME)
	@Target({})
	@interface Field {
		String name();
		Class<?> type();
		String description() default "";
		boolean required() default true;
		boolean nullable() default false;
		/** Allowed string values, such as READY and NOT_READY. */
		String[] values() default {};
		String format() default "";
	}

	/** Container for repeated Returns annotations. */
	@Documented
	@Retention(RetentionPolicy.RUNTIME)
	@Target({ElementType.TYPE, ElementType.METHOD})
	@interface List {
		Returns[] value();
	}
}
