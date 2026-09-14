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

/**
 * A single OpenAI SSE conversation being assembled from a running model job.
 *
 * <p>
 * The protocol state that spans chunks - sequence numbers, the open item and
 * content part, accumulated text, captured usage - lives in the implementation,
 * so the same instance can be driven two ways:
 *
 * <ul>
 * <li>by a caller holding an open http response, which loops on
 * {@link #drain(Writer)} until it returns true and writes straight to the
 * client, and</li>
 * <li>by a caller polling across separate requests, which drains into a fresh
 * buffer each time and hands the bytes back before calling again.</li>
 * </ul>
 *
 * Both produce identical bytes because both run the same per-chunk logic.
 */
public interface IOpenAISseStream {

	/**
	 * Write whatever SSE is ready for the caller right now. Returns without
	 * blocking when the model has produced nothing new.
	 *
	 * @param writer the sink for the SSE text
	 * @return true when the conversation is finished and no further drain is needed
	 * @throws IOException if writing to {@code writer} fails
	 */
	boolean drain(Writer writer) throws IOException;

	/**
	 * @return true once the terminal events have been written
	 */
	boolean isComplete();

	/**
	 * Release the underlying job. Safe to call more than once.
	 */
	void close();
}
