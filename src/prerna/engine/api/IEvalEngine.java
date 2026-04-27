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
package prerna.engine.api;

import java.util.List;

import prerna.reactor.agent.eval.AgentEvalContext;
import prerna.reactor.agent.eval.EvalResult;
import prerna.reactor.agent.eval.EvalSpec;

/**
 * Engine interface for evaluating agent execution traces.
 *
 * <p>Eval engines are meta-system components — they assess execution <em>after the fact</em>.
 * They are NOT tools that agents call during execution. This separation implements the
 * article's core principle: <strong>agents execute, meta-systems learn</strong>.
 *
 * <p>Contrast with {@link IFunctionEngine} (callable tools during agent execution) and
 * {@link IGuardrailReactorFunctionEngine} (content-level safety filtering during execution).
 * Eval engines belong to {@code CATALOG_TYPE.EVAL} — a distinct engine type with its own
 * security namespace, .smss config structure, and discovery path.
 *
 * <h3>Usage</h3>
 * <pre>
 * // Via Pixel:
 * RunEval(traceId=["abc123"], evalEngineId=["eval_task_success_default"])
 * RunEvalSuite(traceId=["abc123"], evalEngineIds=["eval1", "eval2"])
 * </pre>
 *
 * <h3>Implementation Notes</h3>
 * <ul>
 *   <li>Implement {@link #open(java.util.Properties)} to parse eval criteria from .smss config.
 *   <li>Eval criteria belong in config, not prompts — they must be version-controlled and auditable.
 *   <li>Do not call {@link IModelEngine} inside an eval engine — evaluation should be deterministic.
 *       LLM-as-judge evaluators can be built but must be explicitly typed as {@code LLM_JUDGE} subtype.
 * </ul>
 *
 * <p><strong>Phase 2 — not yet registered in {@code CATALOG_TYPE}.</strong> Full lifecycle
 * (Utility dispatch, SecurityEngineUtils, engine folder layout) is tracked in the Phase 2 backlog.
 *
 * @see AgentEvalContext
 * @see EvalResult
 * @see EvalSpec
 */
public interface IEvalEngine extends IEngine {

    /**
     * Evaluates a single agent execution trace.
     *
     * @param evalContext immutable context carrying input, trace, room/model IDs, and task spec
     * @return evaluation result with pass/fail determinations and failure details
     */
    EvalResult evaluate(AgentEvalContext evalContext);

    /**
     * Returns the eval specification — what this engine checks and how it scores.
     *
     * @return eval spec (never null after {@link #open} is called)
     */
    EvalSpec getEvalSpec();

    /**
     * Evaluates a batch of traces. Default implementation delegates to {@link #evaluate} serially;
     * override for parallel or vectorized evaluation.
     *
     * @param contexts list of eval contexts
     * @return list of results in the same order as input contexts
     */
    default List<EvalResult> evaluateBatch(List<AgentEvalContext> contexts) {
        return contexts.stream()
                .map(this::evaluate)
                .collect(java.util.stream.Collectors.toList());
    }
}
