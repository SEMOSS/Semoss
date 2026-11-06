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
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Declares what a reactor returns, so the SDKs can type its output. Without it
 * the output is published as any value: a reactor's code does not say what it
 * returns, so it cannot be inferred.
 *
 * <pre>{@code
 * @PixelOutput(value = String.class, description = "The id of the current insight")
 * public class GetCurrentInsightIdReactor extends AbstractReactor {
 * }</pre>
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface PixelOutput {

	/**
	 * The type of the output: {@code String}, {@code Integer}, {@code Long},
	 * {@code Double}, {@code Boolean}, an enum, {@code Map}, {@code List} (with
	 * {@link #items()}), a record, or {@code Object} for any value.
	 */
	Class<?> value();

	/** The element type when {@link #value()} is {@code List}. */
	Class<?> items() default Void.class;

	/** What the output is. */
	String description() default "";
}
