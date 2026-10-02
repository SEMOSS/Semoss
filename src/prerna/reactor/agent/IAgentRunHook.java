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
 * Observer-style hook fired around a single agent run - i.e. one
 * {@link IAgentHarness#execute(AgentRunContext)} call.
 *
 * <p>Hooks live on the immutable {@link prerna.reactor.agent.config.AgentConfig}
 * (see {@code AgentConfig.getRunHooks()}) and are populated by
 * {@code AgentConfigLoader} from {@code WORKSPACE.CONFIG_JSON.hooks[]}. The
 * runtime invocation site is {@code AgentRunner}, which fires the five
 * lifecycle methods on each registered hook in this order:
 * <pre>
 *   onRoomCreation(ctx)        — room loaded, context built, run starting
 *   beforeRun(ctx)             — final pre-flight; may abort by throwing
 *   afterAgentInit(ctx)        — all init done; harness about to execute
 *   [harness.execute(ctx)]     — tool / message hooks fire inside this
 *   afterRun(ctx, result)      — harness returned (success or failure)
 *   beforeAgentDeInit(ctx, r)  — last chance with workspace overlay active
 * </pre>
 * All call sites are inside the workspace-overlay try-block so hooks see
 * the per-call {@code workspace_id}; the overlay is restored in a
 * {@code finally} after {@code beforeAgentDeInit}.
 *
 * <p>{@link #beforeRun} is the only veto hook — throwing aborts the run
 * and propagates to the caller (skipping subsequent hooks and the harness).
 * Exceptions from {@link #onRoomCreation}, {@link #afterAgentInit},
 * {@link #afterRun}, and {@link #beforeAgentDeInit} are logged and
 * swallowed by the runner so that an observability hook failure never
 * masks a successful agent result.
 *
 * <p>To add a new hook: implement this interface and register it via
 * {@link prerna.reactor.agent.hooks.AgentHookRegistry#register(String, java.util.function.Supplier)}.
 * The registry is the single source of truth shared by
 * {@code AgentConfigLoader.resolveHook} (read path) and
 * {@code AbstractWorkspaceReactor.validateHooks} (write-time validation, used
 * by {@code EditWorkspace}).
 */
public interface IAgentRunHook extends IAgentHook {

    /** Fires after the room is loaded and {@link AgentRunContext} is built, before {@link #beforeRun}. Exceptions are logged and swallowed by the runner. Default: no-op. */
    default void onRoomCreation(AgentRunContext ctx) throws Exception {}

    /** Fires before {@link IAgentHarness#execute(AgentRunContext)}. Throw to abort the run. Default: no-op. */
    default void beforeRun(AgentRunContext ctx) throws Exception {}

    /** Fires after {@link #beforeRun} resolves but before {@link IAgentHarness#execute(AgentRunContext)} starts — i.e. all init is complete and the harness is about to run. Exceptions are logged and swallowed by the runner. Default: no-op. */
    default void afterAgentInit(AgentRunContext ctx) throws Exception {}

    /** Fires after {@link IAgentHarness#execute(AgentRunContext)} returns, with the produced result (may be {@code null} on harness failure). Exceptions are logged and swallowed by the runner. Default: no-op. */
    default void afterRun(AgentRunContext ctx, AgentHarnessResult result) throws Exception {}

    /** Fires after {@link #afterRun} completes, while the workspace overlay is still active, just before the runner restores the overlay. Exceptions are logged and swallowed by the runner. Default: no-op. */
    default void beforeAgentDeInit(AgentRunContext ctx, AgentHarnessResult result) throws Exception {}
}
