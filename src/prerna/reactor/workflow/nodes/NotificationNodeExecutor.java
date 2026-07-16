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

import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.Map;


import prerna.notifications.NotificationDbUtils;
import prerna.reactor.workflow.WorkflowExecutionUtils;

/**
 * Executes a "notification" node: creates a native SEMOSS notification for a recipient,
 * reusing {@link NotificationDbUtils#insertNotification(String, String, String, String, String,
 * String, String, String, String, String, String, String, String)} directly (no Pixel involved).
 *
 * <p>Deliberately does NOT resolve the notification DB itself via
 * {@code SystemEngineRegistry.getNotificationDb()} - that registry enforces a per-engine caller
 * package allowlist that does not include {@code prerna.reactor.workflow.nodes} (or the previous
 * {@code prerna.reactor.workflow}), so calling it directly from here would throw a
 * {@code SecurityException}. The {@code NotificationDbUtils} overload above resolves it
 * internally from within its own (allowed) package instead.
 */
public final class NotificationNodeExecutor implements IWorkflowNodeExecutor {

	@Override
	@SuppressWarnings("unchecked")
	public Object execute(WorkflowNodeContext ctx) {
		Map<String, Object> node = ctx.node();
		Map<String, String> scope = ctx.scope();
		Map<String, String> configMap = ctx.configMap();
		Map<String, Object> config = (Map<String, Object>) node.get("config");

		String recipientId = WorkflowExecutionUtils.resolve(WorkflowExecutionUtils.strCfg(config.get("recipientId")), scope, configMap);
		String title = WorkflowExecutionUtils.resolve(WorkflowExecutionUtils.strCfg(config.get("title")), scope, configMap);
		String message = config.get("message") != null
				? WorkflowExecutionUtils.resolve(WorkflowExecutionUtils.strCfg(config.get("message")), scope, configMap) : "";
		String priority = WorkflowExecutionUtils.strCfg(config.getOrDefault("priority", "MEDIUM"));

		if (recipientId == null || recipientId.isBlank()) throw new IllegalArgumentException("Notification node: 'recipientId' is required");
		if (title == null || title.isBlank()) throw new IllegalArgumentException("Notification node: 'title' is required");

		String notifId;
		try {
			notifId = NotificationDbUtils.insertNotification(recipientId, "NATIVE",
					title, message, priority, "WORKFLOW", null,
					"WORKFLOW", "WORKFLOW", recipientId, "NATIVE", null, null);
		} catch (SQLException e) {
			throw new IllegalStateException("Failed to create notification: " + e.getMessage(), e);
		}

		Map<String, Object> result = new LinkedHashMap<>();
		result.put("sent", true);
		result.put("notificationId", notifId);
		result.put("recipientId", recipientId);
		return WorkflowExecutionUtils.GSON.toJson(result);
	}
}
