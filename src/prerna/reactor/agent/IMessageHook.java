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
package prerna.reactor.agent;

/**
 * Observer-style hook fired around a single agent message — i.e. one
 * {@link IAgentHarness#execute(AgentRunContext)} call.
 *
 * <p>Hooks are returned by {@link AppBuildingHarness#getMessageHooks()} and
 * invoked in list order. {@link #beforeMessage} fires before
 * {@code doExecute}; {@link #afterMessage} fires only after a successful
 * {@code doExecute} return. If any hook throws, subsequent hooks in the
 * chain are skipped and the exception propagates to the caller.
 */
public interface IMessageHook {

    /** Fires before {@code doExecute}. Throw to abort the message. Default: no-op. */
    default void beforeMessage(AgentRunContext ctx) throws Exception {}

    /** Fires after a successful {@code doExecute}, with the produced result. Default: no-op. */
    default void afterMessage(AgentRunContext ctx, AgentHarnessResult result) throws Exception {}
}
