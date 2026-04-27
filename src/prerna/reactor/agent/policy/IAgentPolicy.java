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
/*******************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); ...
 *******************************************************************************/
package prerna.reactor.agent.policy;

import prerna.reactor.agent.AgentHarnessResult;
import prerna.reactor.agent.AgentRunContext;

/**
 * Agent-level policy enforcement — distinct from content-level guardrails.
 *
 * <p>This interface governs task-level execution: whether a run is allowed to start, whether
 * it should continue, and whether its outputs are acceptable.
 *
 * <p>Do NOT conflate with {@link prerna.engine.api.IGuardrailReactorFunctionEngine}, which
 * handles content-level safety (prompt injection, PII, toxic content) during execution.
 * Agent policy operates at the task and budget level, not the content level.
 *
 * <h3>Lifecycle</h3>
 * <pre>
 * 1. preRunCheck  — before the harness starts (permissions, resource budgets)
 * 2. iterationCheck — each tool-call round (budget remaining, deadline)
 * 3. postRunCheck — after harness completes (actions taken, output compliance)
 * </pre>
 *
 * <p><strong>Phase 3 scaffold — implementations pending.</strong>
 * See {@code ProjectAgentPolicy} and {@code PolicyAgentHarness} in the backlog.
 */
public interface IAgentPolicy {

    /**
     * Pre-run check: should this agent run be allowed to start?
     *
     * @param ctx fully-resolved run context
     * @return policy decision (ALLOW, BLOCK, or ESCALATE_TO_HUMAN)
     */
    PolicyDecision preRunCheck(AgentRunContext ctx);

    /**
     * Per-iteration check: should the agent continue to the next tool-call round?
     *
     * @param ctx              run context
     * @param currentIteration iteration number (1-based)
     * @return policy decision
     */
    PolicyDecision iterationCheck(AgentRunContext ctx, int currentIteration);

    /**
     * Post-run check: was the completed run acceptable?
     *
     * @param ctx    run context
     * @param result completed harness result
     * @return policy decision (ALLOW logs the run; BLOCK flags it for audit)
     */
    PolicyDecision postRunCheck(AgentRunContext ctx, AgentHarnessResult result);
}
