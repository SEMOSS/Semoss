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
import java.nio.file.StandardCopyOption;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.zip.ZipFile;

import org.json.JSONArray;
import org.json.JSONObject;

import prerna.reactor.agent.AgentRunContext;
import prerna.reactor.agent.exceptions.AgentCancelledException;

/**
 * Run-owned PPTX state machine. Model text cannot advance or complete these
 * phases.
 */
final class PptxWorkflow {

	static final String TOOL = "BuildPptx";
	static final String PROMPT = """
			SEMOSS manages this PPTX workflow. Save your complete generator as build-deck.js, then call
			BuildPptx alone with generator, filePath, expectedSlides and review instructions. BuildPptx runs
			the generator, independently validates the saved file, and automatically invokes the configured
			PPTX reviewer. ExecuteNodeCode and manual reviewer delegation are unavailable in this workflow.
			For an existing presentation, FIRST call PreparePptxEdit with its exact filename and only the
			requested slide numbers (editType=text for wording changes). It returns the original snapshot,
			slide order and existing text. Load pptx/references/editing.md and use the packaged edit.js
			helper with JSZip in build-deck.js. Never reconstruct an existing deck with PptxGenJS.
			BuildPptx rejects changes outside the prepared scope and restores the previous file on failure.
			Creation examples and PptxGenJS are for new presentations only. The program must save filePath.
			If BuildPptx returns repair_required, apply the listed fixes in one batch and call BuildPptx again
			within the stated repair budget. Prioritize saving over additional edits. SEMOSS delivers the
			recorded outcome automatically; plain text cannot substitute for a saved and checked artifact.
			At the repair limit SEMOSS attempts one final build of changed generator code, then delivers
			the latest validated deck with any outstanding warnings. Protected skill files remain readable.
			""";

	interface Operations {
		JSONObject build(Map<String, Object> arguments) throws Exception;

		JSONObject review(String file, List<Integer> slides, String instructions, String engine) throws Exception;
	}

	private final Path root;
	private final Path stateDirectory;
	private final Operations operations;
	private final PptxEditSession edit;
	private final int repairTurns;
	private String phase = "authoring";
	private String file;
	private String generator;
	private String engine;
	private String instructions;
	private int slides;
	private int builds;
	private int reviews;
	private int repairStarted = -1;
	private int structuralFailures;
	private String lastBuildError;
	private String lastAttemptGeneratorHash;
	private boolean finalBuildAttempted;
	private boolean finalizing;
	private long finalBuildTimeMs;
	private String sourceHash;
	private String generatorHash;
	private String completionError;
	private String completionWarning;
	private boolean artifactAvailable;
	private boolean reviewVerified;
	private String finalText;
	private JSONObject validation;
	private JSONObject savedValidation;
	private JSONObject report;
	private final List<JSONObject> reviewHistory = new ArrayList<>();
	private Map<String, String> packageHashes = Map.of();
	private List<Integer> repairSlides = List.of();
	private java.util.function.Consumer<Map<String, Object>> progress = value -> {
	};

	void onProgress(java.util.function.Consumer<Map<String, Object>> consumer) {
		progress = consumer;
	}

	PptxWorkflow(Path root, Path stateDirectory, int repairTurns, Operations operations) {
		this.root = root.toAbsolutePath().normalize();
		this.stateDirectory = stateDirectory;
		this.repairTurns = Math.max(2, Math.min(12, repairTurns));
		this.operations = operations;
		this.edit = new PptxEditSession(this.root, stateDirectory);
	}

	static PptxWorkflow create(AgentRunContext ctx) {
		var config = ctx.getAgentConfig().getPptxWorkflow();
		Path root = Path.of(ctx.getAgentConfig().getWorkingDir());
		String runId = ctx.getRunId();
		if (runId == null || !runId.matches("[A-Za-z0-9_-]+")) {
			throw new IllegalArgumentException("Managed PPTX workflow requires a run ID");
		}
		PptxWorkflow workflow = new PptxWorkflow(root, root.resolve(".semoss/pptx-workflow/" + runId),
				((Number) config.getOrDefault("repair_turns", 6)).intValue(), new PptxWorkflowOperations(ctx));
		if (ctx.isResumeMode()) {
			workflow.restore();
		} else {
			workflow.captureInputs();
		}
		return workflow;
	}

	void captureInputs() {
		try {
			edit.capture();
		} catch (Exception e) {
			throw new IllegalStateException("Cannot preserve original PowerPoint inputs", e);
		}
	}

	static Map<String, Object> editToolDefinition() {
		JSONObject props = new JSONObject()
				.put("filePath",
						property("string", "Authoritative existing .pptx filename relative to the working directory."))
				.put("outputFilePath",
						property("string", "Exact requested output filename. Omit to edit filePath in place."))
				.put("slides", new JSONObject().put("type", "array")
						.put("items", property("integer", "Original display-order slide number").put("minimum", 1))
						.put("minItems", 1).put("maxItems", 100).put("uniqueItems", true).put("description",
								"Only slides the user requested to change. Do not broaden scope to fix unrelated warnings."))
				.put("editType", property("string",
						"text preserves all formatting and objects; slides permits requested layout/object changes on selected slides.")
						.put("enum", List.of("text", "slides")).put("default", "text"))
				.put("additionalParts", new JSONObject().put("type", "array")
						.put("items", property("string", "Existing package part")).put("description",
								"For slides mode only: exact existing chart, workbook, media, notes or relationship parts required by the request. Shared parts require all affected slides in scope. Usually omit."));
		return new JSONObject().put("name", PptxEditSession.TOOL).put("title", "Prepare an existing PowerPoint edit")
				.put("description",
						"Call before editing an existing PPTX. Inspect the original slide text/order and lock the preservation scope. Returns a protected input snapshot and an editing recipe. Call alone; then save build-deck.js and call BuildPptx.")
				.put("inputSchema",
						new JSONObject().put("type", "object").put("properties", props)
								.put("required", List.of("filePath", "slides")).put("additionalProperties", false))
				.put("_meta", new JSONObject().put("SMSS_TOOL_KIND", "semoss_pptx_workflow").put("SMSS_MCP_EXECUTION",
						"auto"))
				.toMap();
	}

	JSONObject prepareEdit(Map<String, Object> args) {
		try {
			if (isTerminal() || builds > 0) {
				throw new IllegalArgumentException("Prepare edits before the first build");
			}
			String source = required(args, "filePath");
			String output = args.get("outputFilePath") == null ? source : required(args, "outputFilePath");
			source = root.relativize(resolve(source, ".pptx")).toString();
			output = root.relativize(resolve(output, ".pptx")).toString();
			JSONObject result = edit.prepare(source, output, args);
			phase = "editing";
			persist();
			return result;
		} catch (RuntimeException e) {
			throw e;
		} catch (Exception e) {
			throw new IllegalArgumentException("Cannot prepare PowerPoint edit: " + e.getMessage(), e);
		}
	}

	static Map<String, Object> toolDefinition() {
		JSONObject props = new JSONObject()
				.put("generator",
						property("string",
								"Saved JavaScript generator relative to the working directory, usually build-deck.js."))
				.put("filePath",
						property("string", "Exact requested output .pptx filename, relative to the working directory."))
				.put("expectedSlides",
						property("integer", "Requested number of slides. Preserve this count on repairs.")
								.put("minimum", 1).put("maximum", 100))
				.put("instructions", property("string",
						"Visual review criteria and design requirements. The original user brief is included automatically."))
				.put("engine", property("string",
						"Optional exact vision engine ID supplied by the caller. Omit to use the regular reviewer's configured default."));
		return new JSONObject().put("name", TOOL).put("title", "Build and review PowerPoint").put("description",
				"Run a saved generator, validate the PPTX, and automatically review it. Call alone. Returns only bounded repair instructions when needed; otherwise SEMOSS delivers the saved file and actual check results.")
				.put("inputSchema",
						new JSONObject().put("type", "object").put("properties", props)
								.put("required", new JSONArray(List.of("generator", "filePath", "expectedSlides")))
								.put("additionalProperties", false))
				.put("_meta", new JSONObject().put("SMSS_TOOL_KIND", "semoss_pptx_workflow").put("SMSS_MCP_EXECUTION",
						"auto"))
				.toMap();
	}

	private void restore() {
		Path saved = stateDirectory.resolve("state.json");
		try {
			edit.restore(null);
			if (!Files.exists(saved)) {
				return;
			}
			JSONObject value = new JSONObject(Files.readString(saved));
			edit.restore(value.optJSONObject("edit"));
			file = value.optString("filePath", null);
			generator = value.optString("generator", null);
			engine = value.optString("engine", null);
			instructions = value.optString("instructions", null);
			slides = value.optInt("slideCount");
			builds = value.optInt("builds");
			reviews = value.optInt("reviews");
			repairStarted = value.optInt("repairStarted", -1);
			structuralFailures = value.optInt("structuralFailures");
			lastBuildError = value.optString("lastBuildError", null);
			lastAttemptGeneratorHash = value.optString("lastAttemptGeneratorHash", null);
			finalBuildAttempted = value.optBoolean("finalBuildAttempted");
			finalBuildTimeMs = value.optLong("finalBuildTimeMs");
			sourceHash = value.optString("sourceHash", null);
			generatorHash = value.optString("generatorHash", null);
			validation = value.optJSONObject("validation");
			savedValidation = value.optJSONObject("savedValidation");
			report = value.optJSONObject("review");
			completionError = value.optString("error", null);
			finalText = value.optString("finalText", null);
			completionWarning = value.optString("warning", null);
			artifactAvailable = value.optJSONObject("artifact") != null
					&& "available".equals(value.getJSONObject("artifact").optString("status"));
			reviewVerified = value.optJSONObject("reviewOutcome") != null
					&& "complete".equals(value.getJSONObject("reviewOutcome").optString("status"));
			phase = value.getString("phase");
			repairSlides = new ArrayList<>(slideSet(value.optJSONArray("repairSlides")));
			if (value.optJSONArray("reviewHistory") != null) {
				for (Object item : value.getJSONArray("reviewHistory")) {
					reviewHistory.add((JSONObject) item);
				}
			}
			Map<String, String> hashes = new TreeMap<>();
			if (value.optJSONObject("packageHashes") != null) {
				value.getJSONObject("packageHashes").toMap().forEach((k, v) -> hashes.put(k, String.valueOf(v)));
			}
			packageHashes = hashes;
			if (file != null) {
				resolve(file, ".pptx");
			}
			if (generator != null) {
				resolve(generator, ".js");
			}
			if (Set.of("saving", "validating", "visual_review").contains(phase)) {
				finish("Interrupted build/review cannot be treated as completed or retried automatically.");
			}
		} catch (Exception e) {
			throw new IllegalStateException("Cannot restore PPTX workflow evidence", e);
		}
	}

	private static JSONObject property(String type, String description) {
		return new JSONObject().put("type", type).put("description", description);
	}

	boolean isTerminal() {
		return finalText != null;
	}

	String finalText() {
		return finalText;
	}

	String completionError() {
		return completionError;
	}

	String completionWarning() {
		return completionWarning;
	}

	String phase() {
		return phase;
	}

	String guidance(int rounds) {
		if (repairStarted < 0) {
			return "PPTX phase: " + phase
					+ ". For an existing deck call PreparePptxEdit first, use its snapshot with the editing helper, then BuildPptx. For a new deck save your generator, then BuildPptx.";
		}
		return "PPTX phase: " + phase + ". Repair rounds remaining: "
				+ Math.max(0, repairTurns - (rounds - repairStarted))
				+ ". Apply only the requested repairs and call BuildPptx. At the limit SEMOSS will attempt one final build of changed generator code and deliver the latest validated deck.";
	}

	/** Must be called on the harness thread; BuildPptx is never a parallel tool. */
	JSONObject build(Map<String, Object> args, int round) {
		if (isTerminal()) {
			return toolResult();
		}
		try {
			establishTarget(args);
			phase = "saving";
			artifactAvailable = false;
			persist();
			builds++;
			lastAttemptGeneratorHash = hash(resolve(generator, ".js"));
			Map<String, Object> buildArgs = new LinkedHashMap<>(
					Map.of("generator", generator, "filePath", file, "expectedSlides", slides));
			if (edit.active()) {
				buildArgs.put("inputSnapshot", editSnapshot());
			}
			validation = operations.build(buildArgs);
			phase = "validating";
			if (!validation.optBoolean("ok") || validation.optInt("slides") != slides) {
				return structuralFailure("Structural validation failed: " + validation.optJSONArray("errors"), round);
			}
			Path source = resolve(file, ".pptx");
			String actualHash = hash(source);
			if (!actualHash.equals(validation.optString("sourceHash"))) {
				return structuralFailure("Saved file does not match its structural validation hash", round);
			}
			String actualGeneratorHash = hash(resolve(generator, ".js"));
			if (!actualGeneratorHash.equals(validation.optString("generatorHash"))) {
				return structuralFailure("Generator changed during the build", round);
			}

			Map<String, String> nextHashes = packageHashes(source);
			JSONObject preservation = edit.verify(source);
			if (preservation != null) {
				validation.put("preservation", preservation);
			}
			List<Integer> scope = edit.active() ? new ArrayList<>(slideSet(edit.contract().getJSONArray("slides")))
					: reviews == 0 ? allSlides() : changedSlides(packageHashes, nextHashes);
			sourceHash = actualHash;
			generatorHash = actualGeneratorHash;
			packageHashes = nextHashes;
			savedValidation = new JSONObject(validation.toString());
			lastBuildError = null;
			report = null;
			reviewVerified = false;
			Files.createDirectories(stateDirectory);
			Files.copy(source, stateDirectory.resolve("saved.pptx"), StandardCopyOption.REPLACE_EXISTING);
			if (reviews >= 2) {
				finishReview("Review budget exhausted; this saved version has not been reviewed.");
				return toolResult();
			}
			phase = "visual_review";
			reviews++;
			persist();
			String brief = instructions + "\n\nStructural advisory findings (use visual judgment):\n"
					+ validation.optJSONArray("warnings") + "\nReview original slide numbers " + scope + ".";
			if (edit.active()) {
				brief += "\nThis is a scoped edit of an existing presentation. Preservation checks passed: "
						+ preservation
						+ ". Inspect only the requested edits. Do not request redesign or changes to unrelated slides/objects."
						+ "\nOriginal input advisory warnings (pre-existing, not caused by this edit): "
						+ validation.optJSONArray("baselineWarnings");
			}
			report = operations.review(file, scope, brief, engine);
			reviewHistory.add(new JSONObject(report.toString()));
			if (!sourceHash.equals(hash(source))) {
				restoreSaved();
				finishReview(
						"The PowerPoint changed during review. Restored the validated saved version; its visual review is incomplete.");
			} else if (!validReview(report, scope)) {
				finishReview("Visual review was unavailable or incomplete. " + reviewDetails(report, scope));
			} else {
				repairSlides = significantSlides(report);
				reviewVerified = true;
				if (repairSlides.isEmpty()) {
					finish(null);
				} else if (reviews == 1 && !finalizing) {
					phase = "visual_repair";
					repairStarted = round;
					persist();
				} else {
					finishReview(finalizing ? "Significant visual findings remain in the final saved build."
							: "Significant visual findings remain after the single repair pass.");
				}
			}
		} catch (AgentCancelledException e) {
			try {
				restoreSaved();
			} catch (Exception recovery) {
				e.addSuppressed(recovery);
			}
			throw e;
		} catch (Exception e) {
			if ("visual_review".equals(phase)) {
				finishReview("Visual review could not complete: " + e.getMessage());
			} else {
				return structuralFailure(e.getMessage(), round);
			}
		}
		return toolResult();
	}

	private void establishTarget(Map<String, Object> args) throws Exception {
		String nextFile = required(args, "filePath"), nextGenerator = required(args, "generator");
		Object count = args.get("expectedSlides");
		if (!(count instanceof Number n) || n.doubleValue() != n.intValue() || n.intValue() < 1 || n.intValue() > 100) {
			throw new IllegalArgumentException("expectedSlides must be an integer from 1 to 100");
		}
		nextGenerator = root.relativize(resolve(nextGenerator, ".js")).toString();
		nextFile = root.relativize(resolve(nextFile, ".pptx")).toString();
		edit.requirePrepared(nextFile, n.intValue());
		if (file != null && (!file.equals(nextFile) || !generator.equals(nextGenerator) || slides != n.intValue())) {
			throw new IllegalArgumentException(
					"Keep the original generator, output filename and slide count during repairs");
		}
		if (file == null) {
			file = nextFile;
			generator = nextGenerator;
			slides = n.intValue();
			engine = args.get("engine") == null ? null : String.valueOf(args.get("engine"));
			instructions = args.get("instructions") == null
					? "Check readability, clipping, overlap, visual hierarchy and consistency."
					: String.valueOf(args.get("instructions"));
		} else if (args.get("engine") != null && !String.valueOf(args.get("engine")).equals(engine)) {
			throw new IllegalArgumentException("Keep the original vision engine during a repair cycle");
		}
	}

	private JSONObject structuralFailure(String error, int round) {
		structuralFailures++;
		lastBuildError = error;
		try {
			restoreSaved();
		} catch (Exception e) {
			error += "; recovery failed: " + e.getMessage();
		}
		if (finalizing || structuralFailures > 1 || reviews > 0) {
			finish("Build/validation failed: " + error);
		} else {
			phase = "structural_repair";
			repairStarted = round;
		}
		JSONObject result = toolResult().put("buildError", String.valueOf(error));
		persist();
		return result;
	}

	void afterRound(int rounds, int maxTurns) {
		if (!isTerminal() && repairStarted >= 0 && rounds - repairStarted >= repairTurns) {
			finalizeDelivery("Repair budget exhausted.", rounds);
		}
		if (!isTerminal() && rounds >= maxTurns) {
			finalizeDelivery("Agent tool budget exhausted.", rounds);
		}
		persist();
	}

	void modelStopped(String text) {
		if (!isTerminal()) {
			finalizeDelivery(text != null && text.contains("<tool_call>")
					? "The model returned literal tool-call text before the workflow completed."
					: "The author stopped before completing the required build and review workflow.", 0);
		}
	}

	/**
	 * Finish saving without requesting another author turn or opening another
	 * repair cycle.
	 */
	private void finalizeDelivery(String reason, int round) {
		if (isTerminal()) {
			return;
		}
		if (!finalBuildAttempted && file != null && generator != null) {
			try {
				if (Files.isRegularFile(resolve(generator, ".js"))
						&& !hash(resolve(generator, ".js")).equals(lastAttemptGeneratorHash)) {
					finalBuildAttempted = true;
					finalizing = true;
					long started = System.nanoTime();
					try {
						build(Map.of("generator", generator, "filePath", file, "expectedSlides", slides), round);
					} finally {
						finalBuildTimeMs += (System.nanoTime() - started) / 1_000_000;
						finalizing = false;
						persist();
					}
				}
			} catch (AgentCancelledException e) {
				throw e;
			} catch (Exception e) {
				reason += " Final build could not complete: " + e.getMessage();
			}
		}
		if (!isTerminal()) {
			finish(reason);
		}
	}

	/**
	 * A model or runtime error must not hide a previously validated deliverable.
	 */
	void executionFailed(String reason) {
		if (!isTerminal()) {
			finish("Agent execution stopped: " + reason);
		}
	}

	private boolean validReview(JSONObject value, List<Integer> scope) {
		return "complete".equals(value.optString("status")) && !value.optBoolean("sourceChanged", true)
				&& sourceHash.equals(value.optString("sourceHash")) && slides == value.optInt("slideCount")
				&& new TreeSet<>(scope).equals(slideSet(value.optJSONArray("reviewedSlides")))
				&& new TreeSet<>(scope).equals(slideSet(value.optJSONArray("requestedSlides")))
				&& value.optJSONArray("unreviewedSlides") != null && value.getJSONArray("unreviewedSlides").isEmpty()
				&& file.equals(value.optString("filePath")) && value.optJSONArray("issues") != null
				&& (value.optJSONArray("inconclusiveSlides") == null
						|| value.getJSONArray("inconclusiveSlides").isEmpty())
				&& Set.of("pass", "needs_changes").contains(value.optString("verdict"));
	}

	private List<Integer> significantSlides(JSONObject value) {
		TreeSet<Integer> result = new TreeSet<>();
		for (Object issue : value.getJSONArray("issues")) {
			JSONObject item = (JSONObject) issue;
			if (Set.of("major", "critical").contains(item.optString("severity"))) {
				int slide = item.optInt("slide");
				if (slide < 1 || slide > slides) {
					throw new IllegalArgumentException("Reviewer returned an invalid slide number");
				}
				result.add(slide);
			}
		}
		return new ArrayList<>(result);
	}

	private static Set<Integer> slideSet(JSONArray values) {
		TreeSet<Integer> result = new TreeSet<>();
		if (values != null) {
			for (Object value : values) {
				if (!(value instanceof Number n) || n.doubleValue() != n.intValue()) {
					return Set.of(-1);
				}
				if (!result.add(n.intValue())) {
					return Set.of(-1);
				}
			}
		}
		return result;
	}

	/**
	 * Expand the recheck whenever a repair affects shared resources or additional
	 * slides.
	 */
	private List<Integer> changedSlides(Map<String, String> before, Map<String, String> after) {
		TreeSet<Integer> changed = new TreeSet<>(repairSlides);
		TreeSet<String> parts = new TreeSet<>(before.keySet());
		parts.addAll(after.keySet());
		for (String part : parts) {
			if (java.util.Objects.equals(before.get(part), after.get(part))) {
				continue;
			}
			var matcher = java.util.regex.Pattern.compile("ppt/slides/(?:_rels/)?slide(\\d+)\\.xml(?:\\.rels)?")
					.matcher(part);
			if (!matcher.matches()) {
				return allSlides();
			}
			changed.add(Integer.parseInt(matcher.group(1)));
		}
		return new ArrayList<>(changed);
	}

	private List<Integer> allSlides() {
		return java.util.stream.IntStream.rangeClosed(1, slides).boxed().toList();
	}

	private void restoreSaved() throws Exception {
		edit.recoverSeparateSource();
		Path saved = stateDirectory.resolve("saved.pptx");
		if (sourceHash != null && Files.isRegularFile(saved)) {
			if (!sourceHash.equals(hash(saved))) {
				throw new IllegalStateException("Saved artifact recovery hash mismatch");
			}
			Files.copy(saved, resolve(file, ".pptx"), StandardCopyOption.REPLACE_EXISTING);
			if (savedValidation != null) {
				validation = new JSONObject(savedValidation.toString());
			}
		} else {
			edit.recoverOriginal();
		}
	}

	private String editSnapshot() {
		return root.relativize(stateDirectory.resolve("inputs/" + edit.contract().getString("sourceHash") + ".pptx"))
				.toString();
	}

	private void finishReview(String warning) {
		finish(warning);
	}

	private void finish(String reason) {
		if (isTerminal()) {
			return;
		}
		boolean available = false;
		boolean unsaved = false;
		try {
			if (sourceHash != null) {
				if (!Files.isRegularFile(resolve(file, ".pptx")) || !sourceHash.equals(hash(resolve(file, ".pptx")))) {
					restoreSaved();
				}
				available = sourceHash.equals(hash(resolve(file, ".pptx")));
			} else {
				edit.recoverOriginal();
			}
		} catch (Exception e) {
			reason = "Saved artifact could not be verified: " + e.getMessage();
		}
		if (available) {
			try {
				unsaved = !generatorHash.equals(hash(resolve(generator, ".js")));
			} catch (Exception e) {
				unsaved = true;
			}
		}
		artifactAvailable = available;
		if (reason == null && !available) {
			reason = "No validated saved presentation is available.";
		}
		if (reason == null && unsaved) {
			reason = "Delivering the last validated build; later generator edits are not included.";
		}
		if (lastBuildError != null && (reason == null || !reason.contains(lastBuildError))) {
			reason = (reason == null ? "" : reason + " ") + "Last build error: "
					+ (lastBuildError.length() > 700 ? lastBuildError.substring(0, 700) + "…" : lastBuildError);
		}
		completionError = available ? null : reason;
		completionWarning = available ? reason : null;
		phase = completionError != null ? "incomplete"
				: completionWarning != null ? "delivered_with_warnings" : "delivered";
		StringBuilder text = new StringBuilder(
				available ? "Saved `" + file + "` (" + slides + " slides). Structural checks passed."
						: "Unable to deliver a validated PowerPoint" + (file == null ? "." : ": `" + file + "`."));
		if (available && edit.active()) {
			text.append(" Requested edit scope preserved; unrelated package content is unchanged.");
		}
		if (completionError != null) {
			text.append("\n\nIncomplete: ").append(completionError);
		} else if (completionWarning != null) {
			text.append("\n\nWarning: ").append(completionWarning);
		}
		if (available && reviewVerified) {
			Set<Integer> coverage = slideSet(report.optJSONArray("reviewedSlides"));
			text.append(coverage.equals(new TreeSet<>(allSlides())) ? " Full-deck visual review completed."
					: " Visual recheck completed for slides " + coverage
							+ ". The revised whole deck was not rechecked.");
			if (report.getJSONArray("issues").isEmpty()) {
				text.append(" No visual issues were reported in that check.");
			} else {
				text.append(significantSlides(report).isEmpty() ? " Advisory visual findings remain."
						: " Significant visual findings remain; this file has not passed visual review.");
			}
		} else if (available) {
			text.append("\n\nThis file is available, but it has not passed visual review.");
		}
		if (report != null && report.optJSONArray("issues") != null && !report.getJSONArray("issues").isEmpty()) {
			List<JSONObject> findings = new ArrayList<>();
			for (Object value : report.getJSONArray("issues")) {
				findings.add((JSONObject) value);
			}
			findings.sort(java.util.Comparator
					.comparingInt(value -> Set.of("major", "critical").contains(value.optString("severity")) ? 0 : 1));
			text.append("\n\nFindings:");
			for (int i = 0; i < Math.min(6, findings.size()); i++) {
				JSONObject finding = findings.get(i);
				String evidence = finding.optString("evidence", finding.optString("category", "Visual issue"));
				if (evidence.length() > 240) {
					evidence = evidence.substring(0, 240) + "…";
				}
				text.append("\n- Slide ").append(finding.optInt("slide")).append(" (")
						.append(finding.optString("severity")).append("): ").append(evidence);
			}
			if (findings.size() > 6) {
				text.append("\n- ").append(findings.size() - 6)
						.append(" more findings are recorded in the review report.");
			}
		}
		if (unsaved) {
			text.append("\n\nGenerator edits since the last successful build are not included in this file.");
		}
		text.append("\n\nWorkflow report: `").append(root.relativize(stateDirectory.resolve("state.json")))
				.append("`.");
		finalText = text.toString();
		persist();
	}

	private String reviewDetails(JSONObject report, List<Integer> scope) {
		List<String> details = new ArrayList<>();
		details.add("status=" + report.optString("status", "missing") + ", verdict="
				+ report.optString("verdict", "missing"));
		if (!sourceHash.equals(report.optString("sourceHash"))) {
			details.add("review source hash does not match the saved file");
		}
		if (!file.equals(report.optString("filePath"))) {
			details.add("review filename does not match the saved file");
		}
		if (slides != report.optInt("slideCount")) {
			details.add("review slide count does not match the saved file");
		}
		if (!new TreeSet<>(scope).equals(slideSet(report.optJSONArray("reviewedSlides")))) {
			details.add("review coverage does not match the requested slides");
		}
		if (!new TreeSet<>(scope).equals(slideSet(report.optJSONArray("requestedSlides")))) {
			details.add("review requestedSlides does not match the requested scope");
		}
		if (report.optBoolean("sourceChanged", true)) {
			details.add("review source was changed or could not be verified");
		}
		if (report.has("consistencyReview")) {
			details.add("overview=" + report.optString("consistencyReview"));
		}
		if (!Set.of("complete", "not_requested").contains(report.optString("consistencyReview"))) {
			JSONArray observations = report.optJSONArray("observations");
			if (observations != null) {
				for (Object value : observations) {
					JSONObject observation = (JSONObject) value;
					if ("consistency".equals(observation.optString("stage"))) {
						details.add(observation.optString("text"));
					}
				}
			}
		}
		for (String key : List.of("errors", "unreviewedSlides", "inconclusiveSlides")) {
			JSONArray values = report.optJSONArray(key);
			if (values != null && !values.isEmpty()) {
				details.add(key + ": " + values);
			}
		}
		JSONArray limitations = report.optJSONArray("limitations");
		if (limitations != null && !limitations.isEmpty()) {
			details.add("limitations: " + limitations);
		}
		String text = String.join("; ", details);
		return text.length() > 700 ? text.substring(0, 700) + "…" : text;
	}

	JSONObject toolResult() {
		return new JSONObject().put("workflow", "pptx").put("phase", phase).put("status", outcome())
				.put("filePath", file).put("slideCount", slides).put("repairTurns", repairTurns)
				.put("issues", report == null ? new JSONArray() : report.optJSONArray("issues"))
				.put("error", completionError).put("warning", completionWarning).put("artifact", artifact())
				.put("reviewOutcome", reviewOutcome());
	}

	private String outcome() {
		return !isTerminal() ? "repair_required"
				: completionError != null ? "incomplete"
						: completionWarning != null ? "complete_with_warnings" : "complete";
	}

	private JSONObject artifact() {
		return new JSONObject().put("status", artifactAvailable ? "available" : "unavailable").put("filePath", file)
				.put("sourceHash", artifactAvailable ? sourceHash : null).put("slideCount", slides)
				.put("structuralValidation", artifactAvailable ? "passed" : "not_verified");
	}

	private JSONObject reviewOutcome() {
		JSONObject result = new JSONObject()
				.put("status", reviewVerified ? "complete" : reviews == 0 ? "not_started" : "incomplete")
				.put("verdict", reviewVerified ? report.optString("verdict", "inconclusive") : "inconclusive");
		if (report != null) {
			result.put("reportedStatus", report.optString("status")).put("reportedVerdict", report.optString("verdict"))
					.put("consistencyReview", report.optString("consistencyReview", "not_reported"))
					.put("reviewedSlides", report.optJSONArray("reviewedSlides"))
					.put("unreviewedSlides", report.optJSONArray("unreviewedSlides"))
					.put("inconclusiveSlides", report.optJSONArray("inconclusiveSlides"));
		}
		return result;
	}

	JSONObject snapshot() {
		JSONObject result = new JSONObject().put("workflow", "pptx").put("phase", phase).put("builds", builds)
				.put("reviews", reviews).put("repairTurns", repairTurns).put("repairStarted", repairStarted)
				.put("structuralFailures", structuralFailures).put("status", outcome())
				.put("lastBuildError", lastBuildError).put("lastAttemptGeneratorHash", lastAttemptGeneratorHash)
				.put("finalBuildAttempted", finalBuildAttempted).put("finalBuildTimeMs", finalBuildTimeMs)
				.put("filePath", file).put("generator", generator).put("slideCount", slides)
				.put("sourceHash", sourceHash).put("generatorHash", generatorHash).put("validation", validation)
				.put("review", report).put("reviewHistory", new JSONArray(reviewHistory))
				.put("repairSlides", repairSlides).put("error", completionError).put("warning", completionWarning)
				.put("finalText", finalText).put("artifact", artifact()).put("reviewOutcome", reviewOutcome())
				.put("engine", engine).put("instructions", instructions).put("packageHashes", packageHashes)
				.put("savedValidation", savedValidation).put("edit", edit.contract());
		return result;
	}

	private void persist() {
		progress.accept(new JSONObject().put("phase", phase).put("filePath", file).put("sourceHash", sourceHash)
				.put("builds", builds).put("reviews", reviews).put("repairTurns", repairTurns)
				.put("error", completionError).put("lastBuildError", lastBuildError)
				.put("finalBuildAttempted", finalBuildAttempted).put("finalBuildTimeMs", finalBuildTimeMs)
				.put("warning", completionWarning).put("artifact", artifact()).put("reviewOutcome", reviewOutcome())
				.toMap());
		try {
			Files.createDirectories(stateDirectory);
			Path temp = stateDirectory.resolve("state.json.tmp");
			Files.writeString(temp, snapshot().toString(2));
			Files.move(temp, stateDirectory.resolve("state.json"), StandardCopyOption.REPLACE_EXISTING);
		} catch (Exception e) {
			throw new IllegalStateException("Cannot persist PPTX workflow evidence", e);
		}
	}

	Path resolve(String relative, String extension) throws Exception {
		if (relative == null || relative.isBlank() || Path.of(relative).isAbsolute() || !relative.endsWith(extension)) {
			throw new IllegalArgumentException("Expected a relative " + extension + " path");
		}
		Path result = root.resolve(relative).normalize();
		if (!result.startsWith(root) || result.startsWith(root.resolve(".semoss"))
				|| result.startsWith(root.resolve(".claude"))) {
			throw new IllegalArgumentException(
					"Output and generator must be ordinary files within the working directory");
		}
		Path ancestor = result;
		while (!Files.exists(ancestor)) {
			ancestor = ancestor.getParent();
		}
		if (!ancestor.toRealPath().startsWith(root.toRealPath())) {
			throw new IllegalArgumentException("Path escapes the working directory through a symlink");
		}
		return result;
	}

	static String hash(Path path) throws Exception {
		return digest(Files.readAllBytes(path));
	}

	private static String digest(byte[] bytes) throws Exception {
		return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes));
	}

	private static Map<String, String> packageHashes(Path path) throws Exception {
		Map<String, String> result = new TreeMap<>();
		try (ZipFile zip = new ZipFile(path.toFile())) {
			for (var entry : java.util.Collections.list(zip.entries())) {
				String name = entry.getName();
				if (!entry.isDirectory() && name.startsWith("ppt/") && !name.startsWith("ppt/notes")) {
					try (var stream = zip.getInputStream(entry)) {
						result.put(name, digest(stream.readAllBytes()));
					}
				}
			}
		}
		return result;
	}

	private static String required(Map<String, Object> args, String key) {
		Object value = args.get(key);
		if (!(value instanceof String s) || s.isBlank()) {
			throw new IllegalArgumentException(key + " is required");
		}
		return s;
	}
}
