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
package prerna.engine.impl.model.openai;

import java.io.IOException;
import java.io.Writer;
import java.util.Map;

/**
 * Assembles the images/generations SSE conversation for one model job: the
 * {@code image_generation.partial_image} frames, the terminal
 * {@code image_generation.completed} event and the {@code [DONE]} marker.
 *
 * <p>
 * Each frame stands on its own, so nothing carries between chunks beyond the
 * generation settings echoed back on every event.
 */
public class OpenAIImagesStream extends AbstractOpenAISseStream {

	private final String outputFormat;
	private final String quality;
	private final String size;

	/**
	 * @param engineId          the model engine id, echoed on every event
	 * @param creationTimestamp the epoch second stamped on every event
	 * @param jobId             the job id returned by
	 *                          {@code ModelPixelInvoker.startAsyncModelRequest}
	 * @param outputFormat      the requested output format, echoed when present
	 * @param quality           the requested quality, echoed when present
	 * @param size              the requested size, echoed when present
	 */
	public OpenAIImagesStream(String engineId, long creationTimestamp, String jobId, String outputFormat,
			String quality, String size) {
		super(engineId, jobId, creationTimestamp);
		this.outputFormat = outputFormat;
		this.quality = quality;
		this.size = size;
	}

	@Override
	protected boolean writeChunk(Map<String, Object> streamObj, Writer writer) throws IOException {
		String streamType = (String) streamObj.get("stream_type");
		@SuppressWarnings("unchecked")
		Map<String, Object> streamData = (Map<String, Object>) streamObj.get("data");
		if (streamData == null || !"media".equalsIgnoreCase(streamType)) {
			return false;
		}

		@SuppressWarnings("unchecked")
		Map<String, Object> mediaInfo = (Map<String, Object>) streamData.get("media_info");
		Object partialIdxObj = streamData.get("partial_image_index");

		if (mediaInfo == null) {
			return streamData.containsKey("finish_reason");
		}

		Object b64Obj = mediaInfo.get("base64Data");
		String b64 = (b64Obj instanceof String) ? (String) b64Obj : null;
		if (b64 == null || b64.isEmpty()) {
			return false;
		}

		if (partialIdxObj != null) {
			int partialIdx = ((Number) partialIdxObj).intValue();
			OpenAIImagesHelper.writePartialImageEvent(writer, b64, partialIdx, this.engineId, this.creationTimestamp,
					this.outputFormat, this.quality, this.size);
			return false;
		}

		OpenAIImagesHelper.writeCompletedEvent(writer, b64, this.engineId, this.creationTimestamp, this.outputFormat,
				this.quality, this.size, null, null);
		writer.write("data: [DONE]\n\n");
		writer.flush();
		return true;
	}
}
