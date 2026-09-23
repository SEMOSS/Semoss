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

import java.util.List;
import java.util.Map;

import org.json.JSONArray;
import org.json.JSONObject;

import prerna.reactor.agent.AgentRunContext;
import prerna.reactor.agent.exceptions.AgentCancelledException;
import prerna.reactor.agent.run.AgentRunService;
import prerna.reactor.agent.subagent.SubAgentDispatcher;

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
				  return {...validation, ...(baseline ? {baselineWarnings: baseline.warnings} : {}), sourceHash: hash(fs.readFileSync(filename)), generatorHash: hash(source)};
				})()
				"""
				.formatted(new JSONObject(args).toString());
		String output = PlatformAgentTools.executeDefaultTool("ExecuteNodeCode",
				Map.of("code", code, "timeout_seconds", 120), ctx);
		if (Thread.currentThread().isInterrupted()) {
			throw new AgentCancelledException();
		}
		if (output.startsWith("Error:")) {
			throw new IllegalStateException(output);
		}
		int marker = output.lastIndexOf("=> ");
		if (marker < 0) {
			throw new IllegalStateException("Generator execution did not return structural validation");
		}
		return new JSONObject(output.substring(marker + 3).trim());
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
		if (engine != null && !engine.isBlank()) {
			parameters.put("engine", engine);
		}
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
				if (!"succeeded".equals(result.optString("status"))) {
					throw new IllegalStateException("Reviewer did not complete: " + result.optString("error"));
				}
				JSONObject report = new JSONObject(result.getString("result"));
				report.put("reviewerRunId", childId);
				if (engine != null && !engine.isBlank() && !engine.equals(report.optString("engine"))) {
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

	private static String bounded(String text, int max) {
		if (text == null) {
			return "";
		}
		return text.length() <= max ? text : text.substring(0, max);
	}
}
