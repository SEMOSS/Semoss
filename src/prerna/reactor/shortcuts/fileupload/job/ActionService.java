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

import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import prerna.om.Insight;
import prerna.sablecc2.PixelRunner;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class ActionService {

	private static final Pattern PIXEL_PATTERN = Pattern.compile("(META\\|)?(\\w+\\s*\\(.*?\\));", Pattern.DOTALL);

	/*
	 * private static final Pattern INPUT =
	 * Pattern.compile(",?\\s*input\\s*=\\s*\\[.*?]", Pattern.DOTALL);
	 * 
	 * private static final Pattern CONFIG =
	 * Pattern.compile(",?\\s*config\\s*=\\s*\\[.*?]", Pattern.DOTALL);
	 */

	private static final Pattern RESULT = Pattern.compile(",?\\s*result\\s*=\\s*\\[.*?]", Pattern.DOTALL);

	public static ExecutionContext execute(Insight insight, String pixel, ExecutionContext ctx) {

		try {

			// ExecutionLogRepository.log(ctx.executionId, id, "EXECUTE attempt=" +
			// attempt);

			/*
			 * // NounStore inputNouns = new NounStore(id); GenRowStruct grs = new
			 * GenRowStruct();
			 * 
			 * Map<String, Object> processedArguments = new HashMap();
			 * processedArguments.put("input", ctx.input); processedArguments.put("meta",
			 * ctx.meta); processedArguments.put("data", ctx.data);
			 * 
			 * grs.add(new NounMetadata(processedArguments, PixelDataType.MAP));
			 * reactor.getNounStore().addNoun(PipelineReactorUtils.ARGUMENTS, grs); //
			 * reactor.setNounStore(inputNouns);
			 * 
			 * NounMetadata resultNoun = reactor.execute(); Map<String, Object> resultMap =
			 * (Map<String, Object>) resultNoun.getValue();
			 */

			PixelRunner innerRunner = insight.runPixel(pixel); // appendResultToPixel(pixel, ctx)

			// pull the inner pixel runner out
			// since FE is not recursive in how it deals with the payload
			NounMetadata nounMetadata = innerRunner.getResults().get(0);

			Map<String, Object> resultMap = (Map<String, Object>) innerRunner.getResults().get(0).getValue();
			String key = resultMap.keySet().stream().findFirst().orElse(null);
			insight.getVarStore().put(key, nounMetadata);
			ctx.result = resultMap;

			// processedArguments.put("data", ctx.data.put("result", resultMap));
			// processedArguments.put("input", ctx.input.put("input",
			// resultMap.get("input")));
			// processedArguments.put("meta", ctx.meta.put("meta", resultMap.get("meta")));

			return ctx;

		} catch (Exception e) {

		}
		return ctx;

	}

	public String appendResultToPixel(String pixel, ExecutionContext ctx) {

		ObjectMapper mapper = new ObjectMapper();

		// String input = null;
		// String config = null;
		String result = null;
		try {

			// Path filePath = (Path) ctx.input.get("input");

			// input = "input=[" + mapper.writeValueAsString(filePath.toUri()) + "]";
			// config = "config=[" + mapper.writeValueAsString(this.config) + "]";
			// ctx.result.get("result") != null ? ctx.result.get("result") : ctx.result)
			result = "result=[" + mapper
					.writeValueAsString(ctx.result.get("result") != null ? ctx.result.get("result") : ctx.result) + "]";

		} catch (JsonProcessingException e) { // TODO Auto-generated catch block
			e.printStackTrace();
		}

		Matcher m = PIXEL_PATTERN.matcher(pixel);
		StringBuffer sb = new StringBuffer();

		while (m.find()) {

			String prefix = m.group(1) == null ? "" : m.group(1);
			String reactor = m.group(2);

			// Remove existing input/config reactor =
			// INPUT.matcher(reactor).replaceAll(""); reactor =
			// CONFIG.matcher(reactor).replaceAll(""); reactor =
			RESULT.matcher(reactor).replaceAll("");

			reactor = reactor.replaceAll(",\\s*,", ",").replaceAll("\\(\\s*,", "(");

			int idx = reactor.lastIndexOf(")");
			String inside = reactor.substring(reactor.indexOf("(") + 1, idx).trim();

			String injection;

			if (inside.isEmpty()) { // Case: FileExtract();
				injection = result;
			} else { // Case: Has other params
				injection = "," + result;
			}

			reactor = reactor.substring(0, idx) + injection + reactor.substring(idx);

			m.appendReplacement(sb, Matcher.quoteReplacement(prefix + reactor + ";"));
		}

		m.appendTail(sb);
		return sb.toString();
	}

}
