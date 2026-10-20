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
package prerna.io.connector.mail;

import java.util.LinkedHashMap;
import java.util.Map;

import prerna.io.connector.ConnectorOutput;

/**
 * One thing attached to a message, without its bytes.
 *
 * <p>
 * Only a file has bytes that can be saved. Outlook can also attach another
 * message or event, and a link to a file in a drive, which is what {@code kind}
 * tells apart.
 * </p>
 *
 * @param id          the provider's id for the attachment, which the attachment
 *                    download takes
 * @param name        the file name
 * @param contentType the media type
 * @param size        the size in bytes
 * @param isInline    whether it is shown in the body, such as an image, rather
 *                    than attached alongside it
 * @param kind        one of {@link #FILE}, {@link #ITEM} or {@link #LINK}
 */
public record MailAttachment(String id, String name, String contentType, Long size, boolean isInline, String kind) {

	/** A file, which has bytes that can be saved. */
	public static final String FILE = "file";

	/** A message or event embedded in this one. */
	public static final String ITEM = "item";

	/** A link to a file that lives in a drive. */
	public static final String LINK = "link";

	/**
	 * @return the attachment as a reactor answers with it
	 */
	public Map<String, Object> toMap() {
		Map<String, Object> output = new LinkedHashMap<>();
		output.put("id", this.id);
		ConnectorOutput.putIfPresent(output, "name", this.name);
		ConnectorOutput.putIfPresent(output, "contentType", this.contentType);
		ConnectorOutput.putIfPresent(output, "size", this.size);
		output.put("isInline", this.isInline);
		output.put("kind", this.kind);
		return output;
	}
}
