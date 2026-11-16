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
package prerna.util;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import com.zaxxer.hikari.HikariConfig;

/**
 * The sandbox rules for HikariCP, whose package project reactor code shares
 * with the platform. A pool connects through its JDBC URL: the settings that
 * have it look a data source up by name, or create one from a class name, are
 * refused, whether they are set one at a time or come in a {@code Properties}.
 */
public final class SandboxedHikari {

	/**
	 * Internal name of this class, which the rewritten calls name.
	 */
	static final String INTERNAL_NAME = "prerna/util/SandboxedHikari";

	/**
	 * The package shared without its sub-packages, which hold HikariCP's
	 * internals.
	 */
	static final Set<String> SHARED_EXACT_PACKAGES = Set.of("com.zaxxer.hikari");

	/**
	 * The setters that make the pool look a data source up by name, or create one
	 * from a class name.
	 */
	private static final Set<String> LOOKUP_SETTERS = Set.of("setDataSourceJNDI", "setDataSourceClassName");

	/**
	 * The same settings as property names.
	 */
	private static final Set<String> LOOKUP_SETTINGS = Set.of("dataSourceJNDI", "dataSourceClassName");

	/**
	 * Calls project reactor code cannot make, by owner then method name.
	 */
	static final Map<String, Set<String>> BLOCKED_CALLS = Map.of("com/zaxxer/hikari/HikariConfig", LOOKUP_SETTERS,
			"com/zaxxer/hikari/HikariDataSource", LOOKUP_SETTERS);

	/**
	 * The constructors that move to the methods of this class, by class, then
	 * constructor descriptor, as {@code [method, descriptor]}: every public
	 * constructor of HikariConfig.
	 */
	static final Map<String, Map<String, String[]>> CONSTRUCTOR_REDIRECTS = Map.of("com/zaxxer/hikari/HikariConfig",
			Map.of("()V", factory(""), "(Ljava/util/Properties;)V", factory("Ljava/util/Properties;"),
					"(Ljava/lang/String;)V", factory("Ljava/lang/String;")));

	private SandboxedHikari() {
	}

	/**
	 * The redirect to {@code hikariConfig} for one HikariConfig constructor.
	 *
	 * @param arguments the constructor's argument descriptors
	 * @return the method and its descriptor
	 */
	private static String[] factory(String arguments) {
		return new String[] { "hikariConfig", "(" + arguments + ")Lcom/zaxxer/hikari/HikariConfig;" };
	}

	/**
	 * Sandboxed replacement for {@code new HikariConfig()}.
	 *
	 * @return an empty pool configuration
	 */
	public static HikariConfig hikariConfig() {
		return new HikariConfig();
	}

	/**
	 * Sandboxed replacement for {@code new HikariConfig(Properties)}: the pool
	 * connects through its JDBC URL, so a setting that has it look a data source up
	 * by name, or create one from a class name, is refused.
	 *
	 * @param properties the pool settings
	 * @return the pool configuration
	 * @throws SecurityException if the settings hold one of the refused ones
	 */
	public static HikariConfig hikariConfig(Properties properties) {
		for (String key : properties.stringPropertyNames()) {
			for (String refused : LOOKUP_SETTINGS) {
				if (refused.equalsIgnoreCase(key)) {
					throw new SecurityException(
							"The HikariCP setting " + refused + " is not permitted in project reactors");
				}
			}
		}
		return new HikariConfig(properties);
	}

	/**
	 * Sandboxed replacement for {@code new HikariConfig(String)}: reads the
	 * properties file under the path rule of {@link SandboxedJavaExecution}, then
	 * applies {@link #hikariConfig(Properties)}.
	 *
	 * @param propertyFileName the properties file
	 * @return the pool configuration
	 * @throws IllegalArgumentException if the file cannot be read
	 */
	public static HikariConfig hikariConfig(String propertyFileName) {
		Properties properties = new Properties();
		try (FileInputStream in = SandboxedJavaExecution.fileInputStream(propertyFileName)) {
			properties.load(in);
		} catch (IOException e) {
			throw new IllegalArgumentException("Cannot read the pool property file " + propertyFileName, e);
		}
		return hikariConfig(properties);
	}
}
