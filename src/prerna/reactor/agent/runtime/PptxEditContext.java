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

import java.util.ArrayList;
import java.util.List;

import prerna.engine.impl.model.message.AbstractMessage;
import prerna.engine.impl.model.message.InputMessage;

/** A bounded record of user requests, without historical generator/tool output. */
final class PptxEditContext {
	static final String PARAM = "pptx_edit_file";
	static final String PROPOSALS_PARAM = "pptx_edit_proposals";

	static String priorRequests(List<AbstractMessage> messages) {
		List<String> requests = new ArrayList<>();
		for (AbstractMessage message : messages) {
			if (!(message instanceof InputMessage input) || !"input".equals(message.getOrnament("agentRunRole"))) continue;
			String text = input.getInputUIPrompt();
			if (text == null || text.isBlank()) continue;
			int brief = text.indexOf("Primary brief: ");
			if (brief >= 0) text = text.substring(brief + "Primary brief: ".length());
			int footer = text.indexOf("\n\nBefore finishing,");
			if (footer >= 0) text = text.substring(0, footer);
			requests.add(text.substring(0, Math.min(1200, text.length())));
		}
		if (requests.isEmpty()) return "";
		List<String> selected = new ArrayList<>();
		selected.add(requests.getFirst());
		for (int i = Math.max(1, requests.size() - 4); i < requests.size(); i++) selected.add(requests.get(i));
		return "\n\nEarlier user requests (bounded excerpts for context; later requests supersede earlier ones). "
				+ "The current saved presentation is authoritative. Inspect its selected slides; do not infer their contents from old requests.\n"
				+ String.join("\n---\n", selected);
	}
}
