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
package prerna.reactor.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marks a reactor class as an MCP (Model Context Protocol) tool.
 * <p>
 * Annotated reactors are discoverable by {@code MakePixelMCPReactor} when using
 * package scanning via the {@code package} parameter. The annotation also declares
 * MCP-specific metadata that is written into the {@code _meta} section of the
 * generated {@code pixel_mcp.json}.
 * <p>
 * When no arguments are provided, the annotation defaults to {@code execution = "auto"},
 * meaning the AI agent can invoke the tool without user confirmation. Use
 * {@code execution = "ask"} for tools that perform mutations or require explicit approval.
 * <p>
 * This is the Java equivalent of the Python {@code @smssutil.mcp_execution()} decorator.
 *
 * <pre>
 * // Bare — auto-execute, UI defaults
 * &#64;MCPTool
 * public class GetFacilitiesReactor extends AbstractReactor { ... }
 *
 * // Explicit ask for sensitive operations
 * &#64;MCPTool(execution = "ask")
 * public class CreateBenefitsClaimReactor extends AbstractReactor { ... }
 *
 * // With UI hints
 * &#64;MCPTool(execution = "auto", displayLocation = "inline", loadingMessage = "Searching...")
 * public class SearchProvidersReactor extends AbstractReactor { ... }
 * </pre>
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface MCPTool {

	/**
	 * Execution mode for this tool.
	 * <ul>
	 * <li>{@code "auto"} — AI can execute without user confirmation (default)</li>
	 * <li>{@code "ask"} — AI must request user approval before executing</li>
	 * <li>{@code "disabled"} — Tool is documented but cannot be executed</li>
	 * </ul>
	 */
	String execution() default "auto";

	/**
	 * Where the tool's UI output is displayed in the playground.
	 * <ul>
	 * <li>{@code "sidebar"} — Rendered in the sidebar panel (default when empty)</li>
	 * <li>{@code "inline"} — Rendered inline within the chat</li>
	 * <li>{@code "hidden"} — Tool output is not displayed</li>
	 * </ul>
	 * Leave empty to use the platform default (sidebar).
	 */
	String displayLocation() default "";

	/**
	 * Custom loading message shown in the UI while the tool is executing.
	 * Leave empty to use the platform's default loading messages.
	 */
	String loadingMessage() default "";

	/**
	 * Portal page path to load for this tool's UI (e.g., {@code "/claims-form"}).
	 * Maps to: {@code {MODULE}/public_home/{app}/portals{resourceURI}}.
	 * Leave empty to show the default tool form.
	 */
	String resourceURI() default "";

}
