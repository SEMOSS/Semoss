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
 * If your use of this software does not include any GPLv2 components:
 *
 * 	Licensed under the Apache License, Version 2.0 (the "License");
 * 	you may not use this file except in compliance with the License.
 * 	You may obtain a copy of the License at
 *
 * 	   http://www.apache.org/licenses/LICENSE-2.0
 *
 * 	Unless required by applicable law or agreed to in writing, software
 * 	distributed under the License is distributed on an "AS IS" BASIS,
 * 	WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * 	See the License for the specific language governing permissions and
 * 	limitations under the License.
 * ----------------------------------------------------------------------------
 * If your use of this software includes any GPLv2 components:
 *
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
package prerna.engine.impl.guardrail;

import java.util.ArrayList;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import prerna.engine.api.GuardrailTypeEnum;
import prerna.engine.api.IEngine;
import prerna.engine.api.IFunctionEngine;
import prerna.engine.api.IModelEngine;
import prerna.engine.api.ITypeSafeEngine;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.engine.impl.function.FunctionParameter;
import prerna.engine.impl.model.responses.TypeSafeModelEngineResponse;
import prerna.om.Insight;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.NounStore;
import prerna.sablecc2.om.nounmeta.GuardrailNounMetadata;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.Utility;

/**
 * Generic policy guardrail backed by a configured Jev (TypeSafe) model engine.
 * One implementation covers any policy: the criterion is a configurable typed
 * question, and the same engine blocks (or allows) based on configurable
 * decision mapping. A guardrail that checks whether a request is within an
 * approved scope and a guardrail that checks whether a response violates a
 * disclosure policy differ only in their configuration.
 * <p>
 * The evaluation is a {@link ITypeSafeEngine#evaluate(Object, Map, Insight, Map)}
 * call with a structured typed question - a {@code noul} probability or a
 * {@code choice} selection - never a parsed free-text SAFE/UNSAFE answer.
 * <p>
 * The engine calls {@code Utility.getModel(MODEL_ENGINE_ID)} at evaluation
 * time, so the judge model's usage restrictions and token accounting run as for
 * any other user, and the calling user must have view access to the judge.
 * <p>
 * Verdicts are pass/block only. Masking is a separate transformation
 * capability and is out of scope for this engine.
 *
 * <h3>Required SMSS keys</h3>
 * <ul>
 * <li>{@code MODEL_ENGINE_ID} - engine ID of the Jev (TYPESAFE) model used as
 * the judge. Do not attach a pipeline that invokes this guardrail to this
 * model, because that would recurse.</li>
 * <li>{@code QUESTION_INSTRUCTIONS} - the policy criterion the model judges
 * (for example, "Is this request within the approved scope for this
 * assistant?").</li>
 * </ul>
 *
 * <h3>Optional SMSS keys</h3>
 * <ul>
 * <li>{@code QUESTION_TYPE} - {@code NOUL} (default) or {@code CHOICE}.</li>
 * <li>{@code QUESTION_CRITERIA} - required for {@code CHOICE}: a JSON object of
 * choice name to description, e.g.
 * {@code {"APPROVED":"...","MISSING_DISCLOSURE":"..."}}.</li>
 * <li>{@code VIOLATION_CHOICES} - required for {@code CHOICE}: a JSON array
 * naming which criteria choices count as violations, every element of which
 * must appear in the criteria.</li>
 * <li>{@code VIOLATION_DIRECTION} - for {@code NOUL}: {@code YES} (default)
 * when a yes probability of the criterion marks the violation, or {@code NO}
 * when a no does.</li>
 * <li>{@code CONFIDENCE_THRESHOLD} - 0 through 1, default 0.5. A decision is
 * made only when the answer's confidence meets the threshold; the boundary is
 * inclusive, so confidence exactly at the threshold is decided.</li>
 * <li>{@code LOW_CONFIDENCE_VERDICT} - {@code BLOCK} (default) or
 * {@code PASS}. Below-threshold answers never silently become a successful
 * evaluation: by default they block, and the details record the indeterminate
 * outcome.</li>
 * <li>{@code FAIL_OPEN} - whether a judge error (missing model, timeout,
 * malformed or missing answers) allows the guarded call. Defaults to
 * {@code false}, i.e. blocking by default. Set it to {@code true} only where a
 * failed evaluation is preferable to refusing the call.</li>
 * <li>{@code BLOCKED_MESSAGE} - replacement text used by an input pipeline
 * when {@code respondWithGuardrailMessage=true}. Output pipelines can block a
 * failing response but cannot rewrite it.</li>
 * <li>{@code TIMEOUT_SECONDS} - optional seconds to pass to the judge call;
 * a timeout is an evaluation error under the failure policy.</li>
 * </ul>
 *
 * <h3>Optional call parameters</h3>
 * <ul>
 * <li>{@code questionInstructions}, {@code questionType},
 * {@code questionCriteria}, {@code violationChoices},
 * {@code violationDirection}, {@code confidenceThreshold},
 * {@code blockedMessage}, {@code failOpen} - per-call overrides of the engine
 * defaults, supplied through the pipeline's {@code directParameters}. An
 * invalid override or an invalid per-call configuration becomes an evaluation
 * error under the engine's failure policy.</li>
 * </ul>
 */
public class JevPolicyGuardrailEngine extends AbstractGuardrailReactorFunctionEngine {

	private static final Logger classLogger = LogManager.getLogger(JevPolicyGuardrailEngine.class);

	public static final String MODEL_ENGINE_ID_KEY = "MODEL_ENGINE_ID";
	public static final String QUESTION_TYPE_KEY = "QUESTION_TYPE";
	public static final String QUESTION_INSTRUCTIONS_KEY = "QUESTION_INSTRUCTIONS";
	public static final String QUESTION_CRITERIA_KEY = "QUESTION_CRITERIA";
	public static final String VIOLATION_CHOICES_KEY = "VIOLATION_CHOICES";
	public static final String VIOLATION_DIRECTION_KEY = "VIOLATION_DIRECTION";
	public static final String CONFIDENCE_THRESHOLD_KEY = "CONFIDENCE_THRESHOLD";
	public static final String LOW_CONFIDENCE_VERDICT_KEY = "LOW_CONFIDENCE_VERDICT";
	public static final String FAIL_OPEN_KEY = "FAIL_OPEN";
	public static final String BLOCKED_MESSAGE_KEY = "BLOCKED_MESSAGE";
	public static final String TIMEOUT_SECONDS_KEY = "TIMEOUT_SECONDS";

	protected static final String PROMPT_PARAM = "prompt";
	static final String QUESTION_TYPE_PARAM = "questionType";
	static final String QUESTION_INSTRUCTIONS_PARAM = "questionInstructions";
	static final String QUESTION_CRITERIA_PARAM = "questionCriteria";
	static final String VIOLATION_CHOICES_PARAM = "violationChoices";
	static final String VIOLATION_DIRECTION_PARAM = "violationDirection";
	static final String CONFIDENCE_THRESHOLD_PARAM = "confidenceThreshold";
	static final String BLOCKED_MESSAGE_PARAM = "blockedMessage";
	static final String FAIL_OPEN_PARAM = "failOpen";

	static final String QUESTION_TYPE_NOUL = "NOUL";
	static final String QUESTION_TYPE_CHOICE = "CHOICE";
	/** The single named question asked of the judge. */
	static final String QUESTION_NAME = "policy";

	static final String LOW_CONFIDENCE_BLOCK = "BLOCK";
	static final String LOW_CONFIDENCE_PASS = "PASS";

	// per-thread stack of guardrail engine IDs currently evaluating. A pipeline
	// invoked by the judge model runs on the calling thread, so membership here
	// means this guardrail is being evaluated again inside its own evaluation
	private static final ThreadLocal<Deque<String>> ACTIVE_GUARDRAILS = ThreadLocal.withInitial(ArrayDeque::new);

	private static final Gson GSON = new Gson();

	private EngineDefaults defaults;

	@Override
	public void open(Properties smssProp) throws Exception {
		super.open(smssProp);
		this.defaults = fromProperties(smssProp);
		this.functionName = smssProp.getProperty(IFunctionEngine.NAME_KEY);
		this.functionDescription = "Evaluates text against a configurable policy criterion using a typed Jev (TypeSafe) "
				+ "question and returns a pass/block verdict.";
		this.parameters = new ArrayList<>(getGuardrailParameters());
		this.requiredParameters = new ArrayList<>(List.of(PROMPT_PARAM));
	}

	@Override
	public GuardrailNounMetadata execute(NounStore ns, GenRowStruct curRow) {
		return execute(ns, curRow, null);
	}

	@Override
	public GuardrailNounMetadata execute(NounStore ns, GenRowStruct curRow, IEngine targetEngine) {
		Map<String, String> keyValue = organizeKeys(ns, curRow);
		String textToEvaluate = PromptGuardrailEngine
				.extractText(getRawNounValue(ns, curRow, PROMPT_PARAM, 0));
		if (textToEvaluate == null || textToEvaluate.isEmpty()) {
			Map<String, Object> details = new LinkedHashMap<>();
			details.put("outcome", "SKIPPED_NO_TEXT");
			details.put("reason", "No text was mapped to evaluate");
			return new GuardrailNounMetadata(true, textToEvaluate, details);
		}

		Insight insight = insightFromNouns(ns);
		CallConfig call;
		boolean defaultFailOpen = this.defaults != null && this.defaults.failOpen;
		try {
			call = resolveCallConfig(keyValue, this.defaults);
			requireUserContext(insight);
			checkJudgeMount(targetEngine);
		} catch (Exception e) {
			return evaluationFailure(textToEvaluate, e, defaultFailOpen);
		}

		Deque<String> active = ACTIVE_GUARDRAILS.get();
		String guardrailId = getEngineId();
		if (guardrailId != null && active.contains(guardrailId)) {
			return evaluationFailure(textToEvaluate,
					new IllegalStateException("Recursive guardrail evaluation detected for " + guardrailId
							+ " - the judge model's pipeline must not invoke this guardrail"),
					call.failOpen);
		}
		if (guardrailId != null) {
			active.push(guardrailId);
		}
		try {
			ITypeSafeEngine judge = resolveJudgeEngine(insight);
			TypeSafeModelEngineResponse response = evaluatePolicy(judge, textToEvaluate, call, insight);
			Decision decision = decide(response, call);
			if (decision.errored) {
				throw new IllegalStateException(decision.error);
			}
			return buildVerdict(textToEvaluate, call, decision, model(response));
		} catch (Exception e) {
			classLogger.error("{}: judge evaluation failed, failing {}: {}", getClass().getSimpleName(),
					call.failOpen ? "open" : "closed", e.getMessage(), e);
			return evaluationFailure(textToEvaluate, e, call.failOpen);
		} finally {
			if (guardrailId != null) {
				active.pop();
				if (active.isEmpty()) {
					ACTIVE_GUARDRAILS.remove();
				}
			}
		}
	}

	private TypeSafeModelEngineResponse evaluatePolicy(ITypeSafeEngine judge, String textToEvaluate, CallConfig call,
			Insight insight) {
		Map<String, Object> question = new LinkedHashMap<>();
		question.put("type", call.questionType.toLowerCase());
		question.put("instructions", call.instructions);
		if (QUESTION_TYPE_CHOICE.equals(call.questionType)) {
			question.put("criteria", call.criteria);
		}
		Map<String, Object> questions = new LinkedHashMap<>();
		questions.put(QUESTION_NAME, question);
		Map<String, Object> parameters = call.timeoutSeconds == null ? null
				: Map.of("timeout", (Object) call.timeoutSeconds);
		return judge.evaluate(textToEvaluate, questions, insight, parameters);
	}

	/**
	 * Maps a typed judge answer to a verdict.
	 *
	 * @param response the judge's evaluation response
	 * @param call     the resolved configuration
	 * @return the decision, or a decision carrying the error that makes the
	 *         response unusable
	 */
	static Decision decide(TypeSafeModelEngineResponse response, CallConfig call) {
		Map<String, Object> responseMap = response.getResponse();
		if (!(responseMap.get("answers") instanceof Map)) {
			return Decision.error("Jev response did not include answers");
		}
		Map<?, ?> answers = (Map<?, ?>) responseMap.get("answers");
		if (!(answers.get(QUESTION_NAME) instanceof Map)) {
			return Decision.error("Jev response did not include answers." + QUESTION_NAME);
		}
		@SuppressWarnings("unchecked")
		Map<String, Object> answer = (Map<String, Object>) answers.get(QUESTION_NAME);
		if (QUESTION_TYPE_CHOICE.equals(call.questionType)) {
			return decideChoice(answer, call);
		}
		return decideNoul(answer, call);
	}

	static Decision decideNoul(Map<String, Object> answer, CallConfig call) {
		return decideNoul(answer, call.yesViolates, call.confidenceThreshold);
	}

	static Decision decideNoul(Map<String, Object> answer, boolean yesViolates, double threshold) {
		if (!(answer.get("noul") instanceof Number) || !finiteProbability(answer.get("noul"))) {
			return Decision.error("Jev noul answer must be a number from 0 through 1");
		}
		double probabilityYes = ((Number) answer.get("noul")).doubleValue();
		boolean answeredYes = probabilityYes >= 0.5;
		// confidence is the distance from the nearest decision boundary
		double confidence = answeredYes ? probabilityYes : 1 - probabilityYes;
		boolean violated = yesViolates ? answeredYes : !answeredYes;
		boolean determined = confidence >= threshold;
		return Decision.decided(violated, determined, confidence,
				Map.of("probabilityYes", probabilityYes, "answer", answeredYes), answer);
	}

	static Decision decideChoice(Map<String, Object> answer, CallConfig call) {
		return decideChoice(answer, call.violationChoices, call.criteria.keySet(), call.confidenceThreshold);
	}

	static Decision decideChoice(Map<String, Object> answer, Set<String> violationChoices, Set<String> criteriaNames,
			double threshold) {
		if (!(answer.get("choice") instanceof String) || answer.get("choice").toString().isBlank()) {
			return Decision.error("Jev choice answer must name one of the configured choices");
		}
		String choice = answer.get("choice").toString();
		if (!criteriaNames.contains(choice)) {
			return Decision.error("Jev choice answer '" + choice + "' is not one of the configured choices");
		}
		if (!(answer.get("confidence") instanceof Number) || !finiteProbability(answer.get("confidence"))) {
			return Decision.error("Jev choice answer confidence must be a number from 0 through 1");
		}
		double confidence = ((Number) answer.get("confidence")).doubleValue();
		boolean violated = violationChoices.contains(choice);
		boolean determined = confidence >= threshold;
		return Decision.decided(violated, determined, confidence, Map.of("choice", choice), answer);
	}

	private static boolean finiteProbability(Object value) {
		double d = ((Number) value).doubleValue();
		return Double.isFinite(d) && d >= 0 && d <= 1;
	}

	private GuardrailNounMetadata buildVerdict(String textToEvaluate, CallConfig call, Decision decision,
			Object model) {
		Map<String, Object> details = new LinkedHashMap<>();
		details.put("questionType", call.questionType);
		details.put("criterion", call.instructions);
		details.put("judgeModelEngineId", this.defaults.judgeEngineId);
		if (model != null) {
			details.put("model", model);
		}
		details.put("answer", decision.answer);
		details.put("confidence", decision.confidence);
		details.put("confidenceThreshold", call.confidenceThreshold);
		if (QUESTION_TYPE_CHOICE.equals(call.questionType)) {
			details.put("criteria", call.criteria);
			details.put("violationChoices", call.violationChoices);
			details.putAll(decision.typed);
		} else {
			details.put("violationDirection", call.yesViolates ? "YES" : "NO");
			details.putAll(decision.typed);
		}
		details.put("violated", decision.violated);

		boolean pass;
		if (!decision.determined) {
			details.put("outcome", "INDETERMINATE");
			details.put("lowConfidenceVerdict", call.blockOnLowConfidence ? LOW_CONFIDENCE_BLOCK : LOW_CONFIDENCE_PASS);
			details.put("reason", "Confidence " + decision.confidence + " is below threshold "
					+ call.confidenceThreshold + "; the low-confidence policy applied");
			pass = !call.blockOnLowConfidence;
		} else if (decision.violated) {
			details.put("outcome", "VIOLATION");
			details.put("reason", QUESTION_TYPE_CHOICE.equals(call.questionType)
					? "Judge selected violating choice with confidence " + decision.confidence + " >= threshold "
							+ call.confidenceThreshold
					: "Violation probability " + decision.confidence + " meets threshold " + call.confidenceThreshold);
			pass = false;
		} else {
			details.put("outcome", "PASS");
			details.put("reason", QUESTION_TYPE_CHOICE.equals(call.questionType)
					? "Judge selected a non-violating choice with confidence " + decision.confidence
					: "Violation probability is below 0.5 and confidence " + decision.confidence
							+ " meets threshold " + call.confidenceThreshold);
			pass = true;
		}
		classLogger.info("{}: outcome='{}', pass={}", getClass().getSimpleName(), details.get("outcome"), pass);
		String returnPrompt = pass || call.blockedMessage == null ? textToEvaluate : call.blockedMessage;
		return new GuardrailNounMetadata(pass, returnPrompt, details);
	}

	/**
	 * Builds the blocked / error verdict. A policy violation and an evaluation
	 * error never share an outcome value, and an error only allows the guarded
	 * call when the failure policy is explicitly fail-open.
	 *
	 * @param textToEvaluate the text that was under evaluation
	 * @param error          what went wrong
	 * @param failOpen       the failure policy in effect for this call
	 * @return the verdict for the pipeline
	 */
	private GuardrailNounMetadata evaluationFailure(String textToEvaluate, Exception error, boolean failOpen) {
		Map<String, Object> details = new LinkedHashMap<>();
		details.put("outcome", "ERROR");
		details.put("outcomeType", "EVALUATION_ERROR");
		details.put("error", error.getMessage());
		details.put("failPolicy", failOpen ? "OPEN" : "CLOSED");
		details.put("reason", "The judge evaluation failed and the failure policy is fail-"
				+ (failOpen ? "open" : "closed"));
		if (this.defaults != null) {
			details.put("judgeModelEngineId", this.defaults.judgeEngineId);
		}
		if (!failOpen && this.defaults != null && this.defaults.blockedMessage != null) {
			details.put("blockedMessage", this.defaults.blockedMessage);
		}
		return new GuardrailNounMetadata(failOpen, textToEvaluate, details);
	}

	private ITypeSafeEngine resolveJudgeEngine(Insight insight) {
		String judgeEngineId = this.defaults.judgeEngineId;
		if (!SecurityEngineUtils.userCanViewEngine(insight.getUser(), judgeEngineId)) {
			throw new IllegalStateException(
					"User does not have access to the judge model engine " + judgeEngineId);
		}
		IModelEngine judge = Utility.getModel(judgeEngineId);
		if (judge == null) {
			throw new IllegalStateException("Could not find model engine with id: " + judgeEngineId);
		}
		if (!(judge instanceof ITypeSafeEngine)) {
			throw new IllegalStateException("The judge model engine must be a TYPESAFE model engine, "
					+ "but " + judgeEngineId + " is a " + judge.getModelType().getModelName());
		}
		return (ITypeSafeEngine) judge;
	}

	private void checkJudgeMount(IEngine targetEngine) {
		if (targetEngine != null && this.defaults.judgeEngineId.equals(targetEngine.getEngineId())) {
			throw new IllegalStateException("This guardrail is mounted on the pipeline of its own judge model "
					+ this.defaults.judgeEngineId + " - that always recurses");
		}
	}

	private void requireUserContext(Insight insight) {
		if (insight == null || insight.getUser() == null) {
			throw new IllegalStateException(
					"A user context is required to evaluate the Jev policy guardrail: the judge model's usage "
							+ "controls and permissions are enforced against the calling user");
		}
	}

	private Insight insightFromNouns(NounStore ns) {
		if (ns == null) {
			return null;
		}
		GenRowStruct insightGrs = ns.getGenRowStruct(Constants.INSIGHT);
		if (insightGrs != null && !insightGrs.isEmpty()) {
			Object value = insightGrs.get(0);
			if (value instanceof Insight insight) {
				return insight;
			}
			if (value instanceof NounMetadata noun && noun.getValue() instanceof Insight insight) {
				return insight;
			}
		}
		return null;
	}

	private Object getRawNounValue(NounStore ns, GenRowStruct curRow, String key, int positionalIndexIfMissing) {
		if (ns != null) {
			GenRowStruct grs = ns.getGenRowStruct(key);
			if (grs != null && !grs.isEmpty()) {
				if (grs.size() == 1) {
					return grs.get(0);
				}
				List<Object> values = new ArrayList<>();
				for (int i = 0; i < grs.size(); i++) {
					values.add(grs.get(i));
				}
				return values;
			}
		}
		if (curRow != null && !curRow.isEmpty() && positionalIndexIfMissing < curRow.size()) {
			return curRow.get(positionalIndexIfMissing);
		}
		return null;
	}

	private static Object model(TypeSafeModelEngineResponse response) {
		return response.getResponse().get("model");
	}

	List<FunctionParameter> getGuardrailParameters() {
		return List.of(
				new FunctionParameter(PROMPT_PARAM, "String", "The text to evaluate against the policy"),
				new FunctionParameter(QUESTION_TYPE_PARAM, "String", "Optional NOUL or CHOICE question override"),
				new FunctionParameter(QUESTION_INSTRUCTIONS_PARAM, "String", "Optional per-call criterion override"),
				new FunctionParameter(QUESTION_CRITERIA_PARAM, "String",
						"Optional JSON criteria override for CHOICE questions"),
				new FunctionParameter(VIOLATION_CHOICES_PARAM, "String",
						"Optional JSON array of violating choice names for CHOICE questions"),
				new FunctionParameter(VIOLATION_DIRECTION_PARAM, "String",
						"Optional YES or NO violation direction override for NOUL questions"),
				new FunctionParameter(CONFIDENCE_THRESHOLD_PARAM, "Number", "Optional confidence threshold override"),
				new FunctionParameter(BLOCKED_MESSAGE_PARAM, "String", "Optional blocked message override"),
				new FunctionParameter(FAIL_OPEN_PARAM, "Boolean", "Optional failure policy override"));
	}

	@Override
	public GuardrailTypeEnum getGuardrailType() {
		return GuardrailTypeEnum.EMBEDDED_JEV_POLICY;
	}

	@Override
	public String getDefaultMarkdown() {
		return """
				# Jev policy guardrail

				Evaluates text against a configurable policy criterion using a Jev (TypeSafe) model judge.
				The judge answers a typed question, never free text, and the engine maps the typed answer
				to a pass/block verdict. Two different policies - a request-scope check and a response
				disclosure check, for example - need only different configuration, with this same engine.

				## Engine configuration (SMSS)

				```
				MODEL_ENGINE_ID: <id of a TYPESAFE model engine to judge with>
				QUESTION_TYPE: NOUL
				QUESTION_INSTRUCTIONS: Is this request within the approved scope for this assistant?
				```

				All other keys are optional: `VIOLATION_DIRECTION` (YES/NO, for noul), `QUESTION_CRITERIA`
				and `VIOLATION_CHOICES` (both required for choice), `CONFIDENCE_THRESHOLD` (0-1, default
				`0.5`, inclusive boundary), `LOW_CONFIDENCE_VERDICT` (default `BLOCK`), `FAIL_OPEN`
				(default `false` - evaluation errors block the call), `BLOCKED_MESSAGE`, and
				`TIMEOUT_SECONDS`. The judge model must not run a pipeline that invokes this guardrail.

				## Example: block a request outside the approved scope

				Save this as `pipeline.json` in the guarded model engine's assets folder, set
				`PIPELINE pipeline.json` in that model engine's SMSS, and restart or reload it:

				```json
				{
				  "pipelines": {
				    "askRoom": {
				      "input": [
				        {
				          "reactorClass": "prerna.reactor.interceptor.GenericGuardrailInputReactor",
				          "params": {
				            "guardrailEngineId": "%s",
				            "inputMapping": {
				              "prompt": "arg0"
				            },
				            "directParameters": {
				              "questionInstructions": "Is this request within the approved scope: monthly reporting questions only?"
				            },
				            "blockOnGuardrailFailure": true,
				            "respondWithGuardrailMessage": true,
				            "blockErrorMessage": "This assistant only answers monthly reporting questions."
				          }
				        }
				      ]
				    }
				  }
				}
				```

				A blocked request short-circuits the model call entirely - the guarded model is never
				asked, so there is nothing to mask.

				## Example: block a response that violates the disclosure policy

				```json
				{
				  "pipelines": {
				    "askRoom": {
				      "output": [
				        {
				          "reactorClass": "prerna.reactor.interceptor.GenericGuardrailOutputReactor",
				          "params": {
				            "guardrailEngineId": "%s",
				            "inputMapping": {
				              "prompt": "result"
				            },
				            "directParameters": {
				              "questionType": "CHOICE",
				              "questionCriteria": "{\\"APPROVED\\":\\"answer shares approved figures\\",\\"OFF_PLAN\\":\\"answer mentions figures, dates, or customers not on the approved list\\"}",
				              "violationChoices": "[\\"OFF_PLAN\\"]"
				            },
				            "blockErrorMessage": "The response did not pass disclosure review."
				          }
				        }
				      ]
				    }
				  }
				}
				```

				The output reactor maps the completed model response to `prompt` and withholds the result
				when the judge marks it. A violating response is blocked before delivery on a
				non-streaming call. On a streaming call (`llmStreaming` / SSE routes), the model's
				partial output is drained to the client while the guarded method is still running, so
				content chunks are already emitted by the time the output reactor sees the completed
				response - the judge then reviews what was sent, but a violation can no longer be
				recalled. Protecting streamed content at delivery time requires an input-side
				guardrail or a stream-aware check; do not count the output reactor as protection
				for content already emitted.

				## Decision semantics

				- **noul** - the judge returns a violation probability `p` (0-1). `p >= 0.5` answers yes.
				  The confidence is the distance from the boundary. A violation blocks when the answer
				  points at the configured `VIOLATION_DIRECTION` and confidence meets
				  `CONFIDENCE_THRESHOLD`, inclusive.
				- **choice** - the judge selects a configured choice with a confidence (0-1). Selecting a
				  `violationChoices` entry is a violation when the confidence meets the threshold,
				  inclusive.
				- **below threshold** - never a silent pass: the configured `LOW_CONFIDENCE_VERDICT`
				  applies (default `BLOCK`).

				Errors - judge missing, not a TYPESAFE engine, timeout, malformed or missing answers,
				invalid overrides - are evaluation errors, not policy violations, and are blocked unless
				`FAIL_OPEN`/`failOpen` is explicitly `true`. The details of every verdict record the
				outcome (`PASS`, `VIOLATION`, `INDETERMINATE`, `ERROR`), the criterion, the typed answer,
				the confidence against the threshold, and the reason.

				Masking a request instead of blocking it is a separate transformation capability and is
				not part of this guardrail.
				"""
				.formatted(getEngineId(), getEngineId());
	}

	// thread-local recursion helpers, package visible for direct testing

	static boolean isActive(String guardrailId) {
		return ACTIVE_GUARDRAILS.get().contains(guardrailId);
	}

	static void markActive(String guardrailId) {
		ACTIVE_GUARDRAILS.get().push(guardrailId);
	}

	static void clearActive(String guardrailId) {
		Deque<String> active = ACTIVE_GUARDRAILS.get();
		active.remove(guardrailId);
		if (active.isEmpty()) {
			ACTIVE_GUARDRAILS.remove();
		}
	}

	/**
	 * Validates the engine's SMSS configuration. Everything here fails loudly at
	 * creation time; none of it can silently disable the guardrail.
	 *
	 * @param props engine smss properties
	 * @return the validated engine defaults
	 */
	static EngineDefaults fromProperties(Properties props) {
		String judgeEngineId = required(props, MODEL_ENGINE_ID_KEY);
		String questionType = normalizeType(optional(props, QUESTION_TYPE_KEY, QUESTION_TYPE_NOUL), QUESTION_TYPE_KEY);
		String instructions = required(props, QUESTION_INSTRUCTIONS_KEY);
		Map<String, Object> criteria = null;
		Set<String> violationChoices = null;
		boolean yesViolates = true;
		if (QUESTION_TYPE_CHOICE.equals(questionType)) {
			criteria = parseJsonMap(required(props, QUESTION_CRITERIA_KEY), QUESTION_CRITERIA_KEY);
			violationChoices = parseChoiceSet(required(props, VIOLATION_CHOICES_KEY), VIOLATION_CHOICES_KEY);
			validateViolationChoices(criteria, violationChoices);
		} else {
			yesViolates = parseYesNo(optional(props, VIOLATION_DIRECTION_KEY, "YES"), VIOLATION_DIRECTION_KEY);
		}
		double confidenceThreshold = parseThreshold(optional(props, CONFIDENCE_THRESHOLD_KEY, "0.5"));
		boolean blockOnLowConfidence = parseLowConfidenceVerdict(
				optional(props, LOW_CONFIDENCE_VERDICT_KEY, LOW_CONFIDENCE_BLOCK).toUpperCase());
		boolean failOpen = parseStrictBoolean(optional(props, FAIL_OPEN_KEY, "false"), FAIL_OPEN_KEY);
		String blockedMessage = optional(props, BLOCKED_MESSAGE_KEY, null);
		Double timeoutSeconds = props.containsKey(TIMEOUT_SECONDS_KEY) ? parseTimeout(optional(props, TIMEOUT_SECONDS_KEY, null)) : null;
		return new EngineDefaults(judgeEngineId, questionType, instructions, criteria, violationChoices, yesViolates,
				confidenceThreshold, blockOnLowConfidence, failOpen, blockedMessage, timeoutSeconds);
	}

	/**
	 * Merges one call's explicit overrides with the engine defaults. Invalid
	 * values throw and the caller turns the exception into an evaluation error
	 * under the engine's failure policy - never into a silently successful
	 * evaluation.
	 *
	 * @param keyValue resolved named parameters for this call
	 * @param defaults validated engine defaults
	 * @return the configuration for this call
	 */
	static CallConfig resolveCallConfig(Map<String, String> keyValue, EngineDefaults defaults) {
		String questionType = normalizeType(overrideOr(keyValue, QUESTION_TYPE_PARAM, defaults.questionType),
				QUESTION_TYPE_PARAM);
		String instructions = overrideOr(keyValue, QUESTION_INSTRUCTIONS_PARAM, defaults.instructions);
		Map<String, Object> criteria = defaults.criteria;
		Set<String> violationChoices = defaults.violationChoices;
		boolean yesViolates = defaults.yesViolates;
		if (QUESTION_TYPE_CHOICE.equals(questionType)) {
			if (hasValue(keyValue.get(QUESTION_CRITERIA_PARAM))) {
				criteria = parseJsonMap(keyValue.get(QUESTION_CRITERIA_PARAM), QUESTION_CRITERIA_PARAM);
			}
			if (hasValue(keyValue.get(VIOLATION_CHOICES_PARAM))) {
				violationChoices = parseChoiceSet(keyValue.get(VIOLATION_CHOICES_PARAM), VIOLATION_CHOICES_PARAM);
			}
			if (criteria == null || criteria.isEmpty()) {
				throw new IllegalArgumentException("A CHOICE question requires criteria");
			}
			if (violationChoices == null || violationChoices.isEmpty()) {
				throw new IllegalArgumentException("A CHOICE question requires " + VIOLATION_CHOICES_KEY);
			}
			validateViolationChoices(criteria, violationChoices);
		} else if (hasValue(keyValue.get(VIOLATION_DIRECTION_PARAM))) {
			yesViolates = parseYesNo(keyValue.get(VIOLATION_DIRECTION_PARAM), VIOLATION_DIRECTION_PARAM);
		}
		double confidenceThreshold = hasValue(keyValue.get(CONFIDENCE_THRESHOLD_PARAM))
				? parseThreshold(keyValue.get(CONFIDENCE_THRESHOLD_PARAM))
				: defaults.confidenceThreshold;
		String blockedMessage = overrideOr(keyValue, BLOCKED_MESSAGE_PARAM, defaults.blockedMessage);
		boolean failOpen = hasValue(keyValue.get(FAIL_OPEN_PARAM))
				? parseStrictBoolean(keyValue.get(FAIL_OPEN_PARAM), FAIL_OPEN_PARAM)
				: defaults.failOpen;
		Double timeoutSeconds = defaults.timeoutSeconds;
		return new CallConfig(questionType, instructions, criteria, violationChoices, yesViolates,
				confidenceThreshold, defaults.blockOnLowConfidence, failOpen, blockedMessage, timeoutSeconds);
	}

	private static String normalizeType(String questionType, String source) {
		String type = questionType == null ? null : questionType.trim().toUpperCase();
		if (!QUESTION_TYPE_NOUL.equals(type) && !QUESTION_TYPE_CHOICE.equals(type)) {
			throw new IllegalArgumentException(source + " must be " + QUESTION_TYPE_NOUL + " or "
					+ QUESTION_TYPE_CHOICE + ", got '" + questionType + "'");
		}
		return type;
	}

	private static void validateViolationChoices(Map<String, Object> criteria, Set<String> violationChoices) {
		for (String violationChoice : violationChoices) {
			if (!criteria.containsKey(violationChoice)) {
				throw new IllegalArgumentException(
						"Violation choice '" + violationChoice + "' is not one of the criteria choices");
			}
		}
	}

	static Map<String, Object> parseJsonMap(String value, String source) {
		try {
			Map<String, Object> map = GSON.fromJson(value, new TypeToken<LinkedHashMap<String, Object>>() {
			}.getType());
			if (map == null || map.isEmpty()) {
				throw new IllegalArgumentException(source + " must be a nonempty JSON object");
			}
			return new LinkedHashMap<>(map);
		} catch (IllegalArgumentException e) {
			throw e;
		} catch (Exception e) {
			throw new IllegalArgumentException(source + " must be a JSON object of choice name to description", e);
		}
	}

	static Set<String> parseChoiceSet(String value, String source) {
		try {
			Set<String> set = GSON.fromJson(value, new TypeToken<LinkedHashSet<String>>() {
			}.getType());
			if (set == null || set.isEmpty()) {
				throw new IllegalArgumentException(source + " must be a nonempty JSON array");
			}
			return new LinkedHashSet<>(set);
		} catch (IllegalArgumentException e) {
			throw e;
		} catch (Exception e) {
			throw new IllegalArgumentException(source + " must be a JSON array of choice names", e);
		}
	}

	private static boolean parseYesNo(String value, String source) {
		String direction = value == null ? null : value.trim().toUpperCase();
		if ("YES".equals(direction)) {
			return true;
		}
		if ("NO".equals(direction)) {
			return false;
		}
		throw new IllegalArgumentException(source + " must be YES or NO, got '" + value + "'");
	}

	private static double parseThreshold(String value) {
		double threshold;
		try {
			threshold = Double.parseDouble(value.trim());
		} catch (RuntimeException e) {
			throw new IllegalArgumentException("Confidence threshold must be a number from 0 through 1, got '"
					+ value + "'");
		}
		if (!Double.isFinite(threshold) || threshold < 0 || threshold > 1) {
			throw new IllegalArgumentException(
					"Confidence threshold must be a number from 0 through 1, got '" + value + "'");
		}
		return threshold;
	}

	private static boolean parseLowConfidenceVerdict(String value) {
		if (LOW_CONFIDENCE_BLOCK.equals(value)) {
			return true;
		}
		if (LOW_CONFIDENCE_PASS.equals(value)) {
			return false;
		}
		throw new IllegalArgumentException(
				LOW_CONFIDENCE_VERDICT_KEY + " must be " + LOW_CONFIDENCE_BLOCK + " or " + LOW_CONFIDENCE_PASS
						+ ", got '" + value + "'");
	}

	private static boolean parseStrictBoolean(String value, String source) {
		String normalized = value == null ? null : value.trim();
		if ("true".equalsIgnoreCase(normalized)) {
			return true;
		}
		if ("false".equalsIgnoreCase(normalized)) {
			return false;
		}
		throw new IllegalArgumentException(source + " must be true or false, got '" + value + "'");
	}

	private static Double parseTimeout(String value) {
		Double timeout;
		try {
			timeout = Double.parseDouble(value.trim());
		} catch (RuntimeException e) {
			throw new IllegalArgumentException(TIMEOUT_SECONDS_KEY + " must be a positive number of seconds, got '"
					+ value + "'");
		}
		if (!Double.isFinite(timeout) || timeout <= 0) {
			throw new IllegalArgumentException(
					TIMEOUT_SECONDS_KEY + " must be a positive number of seconds, got '" + value + "'");
		}
		return timeout;
	}

	private static String overrideOr(Map<String, String> keyValue, String param, String engineDefault) {
		return hasValue(keyValue.get(param)) ? keyValue.get(param) : engineDefault;
	}

	private static boolean hasValue(String value) {
		return value != null && !value.trim().isEmpty();
	}

	private static String required(Properties props, String key) {
		String value = props.getProperty(key);
		if (value == null || value.trim().isEmpty()) {
			throw new IllegalArgumentException(key + " is required for " + JevPolicyGuardrailEngine.class.getSimpleName());
		}
		return value.trim();
	}

	private static String optional(Properties props, String key, String defaultValue) {
		String value = props.getProperty(key);
		return value != null && !value.trim().isEmpty() ? value.trim() : defaultValue;
	}

	/** Validated engine-level configuration, resolved once at open. */
	static final class EngineDefaults {
		final String judgeEngineId;
		final String questionType;
		final String instructions;
		final Map<String, Object> criteria;
		final Set<String> violationChoices;
		final boolean yesViolates;
		final double confidenceThreshold;
		final boolean blockOnLowConfidence;
		final boolean failOpen;
		final String blockedMessage;
		final Double timeoutSeconds;

		private EngineDefaults(String judgeEngineId, String questionType, String instructions,
				Map<String, Object> criteria, Set<String> violationChoices, Boolean yesViolates,
				double confidenceThreshold, boolean blockOnLowConfidence, boolean failOpen, String blockedMessage,
				Double timeoutSeconds) {
			this.judgeEngineId = judgeEngineId;
			this.questionType = questionType;
			this.instructions = instructions;
			this.criteria = criteria;
			this.violationChoices = violationChoices;
			this.yesViolates = yesViolates;
			this.confidenceThreshold = confidenceThreshold;
			this.blockOnLowConfidence = blockOnLowConfidence;
			this.failOpen = failOpen;
			this.blockedMessage = blockedMessage;
			this.timeoutSeconds = timeoutSeconds;
		}
	}

	/** One call's merged configuration. */
	static final class CallConfig {
		final String questionType;
		final String instructions;
		final Map<String, Object> criteria;
		final Set<String> violationChoices;
		final boolean yesViolates;
		final double confidenceThreshold;
		final boolean blockOnLowConfidence;
		final boolean failOpen;
		final String blockedMessage;
		final Double timeoutSeconds;

		private CallConfig(String questionType, String instructions, Map<String, Object> criteria,
				Set<String> violationChoices, Boolean yesViolates, double confidenceThreshold,
				boolean blockOnLowConfidence, boolean failOpen, String blockedMessage, Double timeoutSeconds) {
			this.questionType = questionType;
			this.instructions = instructions;
			this.criteria = criteria;
			this.violationChoices = violationChoices;
			this.yesViolates = yesViolates;
			this.confidenceThreshold = confidenceThreshold;
			this.blockOnLowConfidence = blockOnLowConfidence;
			this.failOpen = failOpen;
			this.blockedMessage = blockedMessage;
			this.timeoutSeconds = timeoutSeconds;
		}
	}

	/** The typed verdict for one evaluation. */
	static final class Decision {
		final boolean errored;
		final String error;
		final boolean violated;
		final boolean determined;
		final double confidence;
		final Map<String, Object> typed;
		final Map<String, Object> answer;

		private Decision(boolean errored, String error, boolean violated, boolean determined, double confidence,
				Map<String, Object> typed, Map<String, Object> answer) {
			this.errored = errored;
			this.error = error;
			this.violated = violated;
			this.determined = determined;
			this.confidence = confidence;
			this.typed = typed;
			this.answer = answer;
		}

		static Decision decided(boolean violated, boolean determined, double confidence,
				Map<String, Object> typed, Map<String, Object> answer) {
			return new Decision(false, null, violated, determined, confidence, typed, answer);
		}

		static Decision error(String error) {
			return new Decision(true, error, false, false, 0.0, Map.of(), Map.of());
		}
	}
}