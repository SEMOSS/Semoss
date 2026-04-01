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
package prerna.reactor.shortcuts.fileupload.job;

public class WorkflowOrchestrator {
	/*
	 * 
	 * private final ExecutorService executor = Executors.newFixedThreadPool(10);
	 * 
	 * private final ActionService actionService; private final DlqRepository
	 * dlqRepository;
	 * 
	 * public WorkflowOrchestrator(ActionService actionService, DlqRepository
	 * dlqRepository) {
	 * 
	 * this.actionService = actionService; this.dlqRepository = dlqRepository; }
	 * 
	 * public CompletableFuture<ExecutionContext> start(Insight insight,
	 * WorkflowDefinition workflow, ExecutionContext ctx) {
	 * 
	 * return executeNode(insight, workflow, workflow.getStartNodeId(), ctx); }
	 * 
	 * private CompletableFuture<ExecutionContext> executeNode(Insight insight,
	 * WorkflowDefinition workflow, String nodeId, ExecutionContext ctx) {
	 * 
	 * if (nodeId == null) { return CompletableFuture.completedFuture(ctx); }
	 * 
	 * Node node = workflow.getNodes().get(nodeId);
	 * 
	 * if (node.isAction()) {
	 * 
	 * return executeWithRetry(insight, node, workflow, ctx, 0)
	 * .thenCompose(updatedCtx -> executeNode(insight, workflow, node.getNext(),
	 * updatedCtx));
	 * 
	 * } else {
	 * 
	 * boolean decision = ConditionEvaluator.evaluateCondition(ctx.result,
	 * node.getCondition());
	 * 
	 * String nextNode = decision ? node.getOnTrue() : node.getOnFalse();
	 * 
	 * return executeNode(insight, workflow, nextNode, ctx);
	 * 
	 * 
	 * WorkflowActionResult workflowActionResult = (WorkflowActionResult)
	 * ctx.result.get("result"); boolean decision = false; if
	 * (workflowActionResult.result.get("FileExtractionResult") instanceof
	 * FileExtractionResult) { FileExtractionResult fileExtractionResult =
	 * (FileExtractionResult) workflowActionResult.result
	 * .get("FileExtractionResult"); decision =
	 * fileExtractionResult.fileType.equalsIgnoreCase(node.getConditionValue()); }
	 * // ;decision = ctx.result.get("result").getOrDefault(node.getConditionKey(),
	 * // "").toString().equals(node.getConditionValue()); //
	 * ctx.getResult().get("result").getOrDefault(node.getConditionKey(), //
	 * "").toString().equals(node.getConditionValue());
	 * 
	 * String next = decision ? node.getOnTrue() : node.getOnFalse();
	 * 
	 * 
	 * // return executeNode(insight, workflow, next, ctx); } }
	 * 
	 * private CompletableFuture<ExecutionContext> executeWithRetry(Insight insight,
	 * Node node, WorkflowDefinition workflow, ExecutionContext ctx, int attempt) {
	 * 
	 * return CompletableFuture.<ExecutionContext>supplyAsync(() -> {
	 * 
	 * try {
	 * 
	 * Object result = actionService.execute(insight, node.getPixel(), ctx);
	 * 
	 * // ctx.getResult().put(node.getNodeId(), result);
	 * 
	 * if (result instanceof java.util.Map<?, ?> map) { map.forEach((k, v) ->
	 * ctx.result.put(node.getNodeId() + "." + k, v)); }
	 * 
	 * return ctx;
	 * 
	 * } catch (Exception e) { throw new CompletionException(e); }
	 * 
	 * }, executor)
	 * 
	 * .exceptionallyCompose(ex -> {
	 * 
	 * if (attempt < workflow.getRetryPolicy().getMaxRetries()) {
	 * 
	 * long delay = calculateBackoff(workflow.getRetryPolicy(), attempt);
	 * 
	 * return ((CompletableFuture<ExecutionContext>)
	 * CompletableFuture.delayedExecutor(delay,
	 * TimeUnit.MILLISECONDS)).supplyAsync(() -> null) .thenCompose(v ->
	 * executeWithRetry(insight, node, workflow, ctx, attempt + 1)); }
	 * 
	 * // dlqRepository.save(workflow.getWorkflowId(), node.getNodeId(), //
	 * ex.getMessage());
	 * 
	 * return CompletableFuture.failedFuture(ex); }); }
	 * 
	 * private long calculateBackoff(RetryPolicy policy, int attempt) {
	 * 
	 * long delay = (long) (policy.getInitialDelayMs() *
	 * Math.pow(policy.getMultiplier(), attempt));
	 * 
	 * return Math.min(delay, policy.getMaxDelayMs()); }
	 */}