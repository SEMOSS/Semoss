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
package prerna.reactor.workflow.nodes;

import java.nio.charset.StandardCharsets;
import java.net.URLEncoder;
import java.util.LinkedHashMap;
import java.util.Map;


import prerna.reactor.workflow.WorkflowExecutionUtils;

/**
 * Executes an "email" node: builds a validated {@code SendEmail(...)} Pixel call from structured
 * config and runs it - the backend owns the mapping from config to Pixel syntax, reusing the
 * existing {@code SendEmailReactor}, rather than trusting a frontend-precompiled Pixel string.
 */
public final class EmailNodeExecutor implements IWorkflowNodeExecutor {

	@Override
	@SuppressWarnings("unchecked")
	public Object execute(WorkflowNodeContext ctx) {
		Map<String, Object> node = ctx.node();
		Map<String, String> scope = ctx.scope();
		Map<String, String> configMap = ctx.configMap();
		Map<String, Object> config = (Map<String, Object>) node.get("config");

		String to = WorkflowExecutionUtils.resolve(WorkflowExecutionUtils.strCfg(config.get("to")), scope, configMap);
		String subject = WorkflowExecutionUtils.resolve(WorkflowExecutionUtils.strCfg(config.get("subject")), scope, configMap);
		String body = WorkflowExecutionUtils.resolve(WorkflowExecutionUtils.strCfg(config.get("body")), scope, configMap);

		if (to == null || to.isBlank()) throw new IllegalArgumentException("Email node: 'to' is required");
		if (subject == null || subject.isBlank()) throw new IllegalArgumentException("Email node: 'subject' is required");
		if (body == null) body = "";

		boolean isHtml = Boolean.parseBoolean(WorkflowExecutionUtils.strCfg(config.getOrDefault("isHtml", "false")));
		String cc = config.get("cc") != null ? WorkflowExecutionUtils.resolve(WorkflowExecutionUtils.strCfg(config.get("cc")), scope, configMap) : null;
		String bcc = config.get("bcc") != null ? WorkflowExecutionUtils.resolve(WorkflowExecutionUtils.strCfg(config.get("bcc")), scope, configMap) : null;

		// URL-encode body so it can be safely embedded in a pixel string
		String encodedBody = URLEncoder.encode(body, StandardCharsets.UTF_8);

		StringBuilder pixel = new StringBuilder("SendEmail(");
		pixel.append("to=[").append(buildEmailAddressParam(to)).append("]");
		if (cc != null && !cc.isBlank()) pixel.append(", cc=[").append(buildEmailAddressParam(cc)).append("]");
		if (bcc != null && !bcc.isBlank()) pixel.append(", bcc=[").append(buildEmailAddressParam(bcc)).append("]");
		pixel.append(", subject=[\"").append(subject.replace("\"", "\\\"")).append("\"]");
		pixel.append(", message=[\"<encode>").append(encodedBody).append("</encode>\"]");
		if (isHtml) pixel.append(", html=[\"true\"]");
		pixel.append(");");

		try {
			ctx.insight().runPixel(pixel.toString());
		} catch (Exception e) {
			throw new IllegalStateException("Email send failed: " + e.getMessage(), e);
		}

		Map<String, Object> result = new LinkedHashMap<>();
		result.put("sent", true);
		result.put("to", to);
		return WorkflowExecutionUtils.GSON.toJson(result);
	}

	private static String buildEmailAddressParam(String addresses) {
		String[] parts = addresses.split("\\s*,\\s*");
		StringBuilder sb = new StringBuilder();
		for (String addr : parts) {
			if (sb.length() > 0) sb.append(", ");
			sb.append("\"").append(addr.trim()).append("\"");
		}
		return sb.toString();
	}
}
