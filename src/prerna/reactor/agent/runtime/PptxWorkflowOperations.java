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
package prerna.reactor.agent.runtime;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.json.JSONArray;
import org.json.JSONObject;

import prerna.reactor.agent.AgentRunContext;
import prerna.reactor.agent.exceptions.AgentCancelledException;
import prerna.reactor.agent.run.AgentRunService;
import prerna.reactor.agent.subagent.SubAgentDispatcher;
import prerna.engine.impl.model.inferencetracking.ModelInferenceLogsUtils;
import prerna.util.pptx.SemossPptxInspector;

/**
 * Existing Node and named-reviewer adapters used by the managed PPTX workflow.
 */
final class PptxWorkflowOperations implements PptxWorkflow.Operations {

	private final AgentRunContext ctx;

	PptxWorkflowOperations(AgentRunContext ctx) {
		this.ctx = ctx;
	}

	@Override
	public JSONObject build(Map<String, Object> args) throws Exception {
		// The report is handed back in a file: ExecuteNodeCode caps its output at 40,000
		// characters, and a long deck's advisory warnings alone exceed that.
		String report = ".semoss/pptx-workflow/" + ctx.getRunId() + "/build-report-" + UUID.randomUUID() + ".json";
		Path reportPath = Path.of(ctx.getAgentConfig().getWorkingDir()).resolve(report);
		Files.createDirectories(reportPath.getParent());
		String code = """
				(async () => {
				  const fs = require('fs'), path = require('path'), crypto = require('crypto');
				  const args = %s;
				  const filename = path.join(ROOT, args.filePath);
				  const generator = path.join(ROOT, args.generator);
				  const stamp = () => fs.existsSync(filename) ? String(fs.statSync(filename, {bigint:true}).mtimeNs) + ':' + String(fs.statSync(filename, {bigint:true}).ctimeNs) : null;
				  const before = stamp();
				  const deck = require(path.join(ROOT, '.claude/skills/pptx/scripts/deck.js'));
				  const baseline = args.inputSnapshot ? deck.validate(path.join(ROOT, args.inputSnapshot), {slides: args.expectedSlides, strictCanvas: false}) : null;
				  const source = fs.readFileSync(generator, 'utf8');
				  await eval(source);
				  if (!fs.existsSync(filename) || stamp() === before) throw new Error('Generator did not save the requested PPTX file');
				  const validation = deck.validate(filename, {slides: args.expectedSlides, strictCanvas: false});
				  const hash = value => crypto.createHash('sha256').update(value).digest('hex');
				  fs.writeFileSync(path.join(ROOT, %s), JSON.stringify({...validation, ...(baseline ? {baselineWarnings: baseline.warnings} : {}), sourceHash: hash(fs.readFileSync(filename)), generatorHash: hash(source)}));
				  return 'Structural validation saved';
				})()
				"""
				.formatted(new JSONObject(args).toString(), JSONObject.quote(report));
		String output = PlatformAgentTools.executeDefaultTool("ExecuteNodeCode",
				Map.of("code", code, "timeout_seconds", 120), ctx);
		if (Thread.currentThread().isInterrupted()) {
			throw new AgentCancelledException();
		}
		// A fresh report means the script finished, whatever the generator printed.
		if (Files.isRegularFile(reportPath)) {
			try {
				return new JSONObject(Files.readString(reportPath));
			} finally {
				Files.deleteIfExists(reportPath);
			}
		}
		if (output.startsWith("Error:")) {
			throw new IllegalStateException(output);
		}
		throw new IllegalStateException("Generator execution did not return structural validation");
	}

	@Override
	public JSONObject review(String file, List<Integer> slides, String instructions, String engine) throws Exception {
		var cfg = ctx.getAgentConfig().getPptxWorkflow();
		String alias = String.valueOf(cfg.getOrDefault("reviewer_alias", "agent_pptx_reviewer"));
		var spec = ctx.getAgentConfig().getSubagents().stream().filter(s -> alias.equals(s.getAlias())).findFirst()
				.orElseThrow(() -> new IllegalStateException("Configured PPTX reviewer is not attached: " + alias));
		if (ctx.getSpawnDepth() >= ctx.getAgentConfig().getSpawnPolicy().getMaxSubagentDepth()) {
			throw new IllegalStateException("PPTX reviewer exceeds this agent's configured spawn depth");
		}
		JSONObject parameters = new JSONObject().put("filePath", file).put("slides", new JSONArray(slides))
				.put("instructions", bounded(instructions, 11000)).put("context", bounded(ctx.getInput(), 11000));
		// The system reviewer has a known InspectPptx contract. Custom reviewers retain their own routing.
		String reviewEngine = engine;
		if ("pptx-reviewer".equals(spec.getWorkspaceId())) {
			JSONObject reviewer = ModelInferenceLogsUtils.getWorkspaceConfigJson(spec.getWorkspaceId());
			String fallback = ctx.getModelEngine().getEngineId();
			if (reviewer != null) {
				if (!reviewer.optString("model_id").isBlank()) fallback = reviewer.getString("model_id");
				JSONObject policy = reviewer.optJSONObject("tool_policy");
				JSONObject defaults = policy == null ? null : policy.optJSONObject("parameter_defaults");
				JSONObject inspection = defaults == null ? null : defaults.optJSONObject("InspectPptx");
				if ((reviewEngine == null || reviewEngine.isBlank()) && inspection != null)
					reviewEngine = inspection.optString("engine", null);
			}
			reviewEngine = SemossPptxInspector.preflight(reviewEngine, fallback, ctx.getInsight());
		}
		if (reviewEngine != null && !reviewEngine.isBlank()) parameters.put("engine", reviewEngine);
		String prompt = "Inspect the saved PowerPoint using InspectPptx with these exact parameters. Return its report unchanged. "
				+ "Do not edit files or ask for human approval.\n" + parameters;
		JSONObject spawned = new JSONObject(
				SubAgentDispatcher.spawnNamed(spec, Map.of("prompt", prompt, "inherit_parent_workdir", true),
						ctx.getRoom(), ctx.getInsight(), ctx.getRunId(), ctx.getAgentConfig().getAuthoredPrompt()));
		String childId = spawned.optString("runId", spawned.optString("jobId"));
		if (childId.isBlank()) {
			throw new IllegalStateException("Could not start reviewer: " + spawned);
		}
		int seconds = Math.max(1, Math.min(900, ((Number) cfg.getOrDefault("review_timeout_seconds", 600)).intValue()));
		long deadline = System.nanoTime() + seconds * 1_000_000_000L;
		boolean ended = false;
		try {
			while (System.nanoTime() < deadline) {
				if (Thread.currentThread().isInterrupted()) {
					throw new AgentCancelledException();
				}
				int wait = (int) Math.max(1, Math.min(10, (deadline - System.nanoTime()) / 1_000_000_000L));
				JSONObject result = new JSONObject(SubAgentDispatcher.wait(childId, ctx.getInsight(), wait));
				if ("running".equals(result.optString("status"))) {
					continue;
				}
				ended = true;
				JSONObject report = parseReviewResult(result, childId);
				if (reviewEngine != null && !reviewEngine.isBlank() && !reviewEngine.equals(report.optString("engine"))) {
					throw new IllegalStateException("Reviewer did not use the requested vision engine ID");
				}
				return report;
			}
			throw new IllegalStateException("Reviewer exceeded the " + seconds + " second review deadline");
		} finally {
			if (!ended) {
				AgentRunService.get().cancelRun(childId, "PPTX parent stopped waiting for review");
			}
		}
	}

	static JSONObject parseReviewResult(JSONObject result, String childId) {
		if (!"succeeded".equals(result.optString("status")))
			throw new IllegalStateException("Reviewer did not complete: " + bounded(result.optString("error", "unknown error"), 1200));
		Object raw = result.opt("result");
		String text = raw instanceof String value ? value.trim() : "";
		if (!text.startsWith("{"))
			throw new IllegalStateException("Reviewer returned an error instead of a report: " + bounded(text.isEmpty() ? "empty result" : text, 1200));
		try { return new JSONObject(text).put("reviewerRunId", childId); }
		catch (org.json.JSONException e) { throw new IllegalStateException("Reviewer returned a malformed JSON report", e); }
	}

	private static String bounded(String text, int max) {
		if (text == null) {
			return "";
		}
		return text.length() <= max ? text : text.substring(0, max);
	}
}
