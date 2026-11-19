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
package prerna.util;

/**
 * Refuses a save made from a copy that is no longer current: someone changed
 * what is being saved after the copy was read. The caller reads it again and
 * makes the change on the latest version. It is an
 * {@link IllegalArgumentException}, so a handler that does not know it still
 * answers with a bad request, and the endpoints that know it answer with a
 * conflict carrying {@link #getErrorCode()}.
 */
public class ContentChangedException extends IllegalArgumentException {

	private static final long serialVersionUID = 1L;

	private final String errorCode;

	/**
	 * @param errorCode what changed, such as {@code SMSS_CHANGED}, for the caller
	 *                  to recognize the refusal by
	 * @param message   the message shown to the user
	 */
	public ContentChangedException(String errorCode, String message) {
		super(message);
		this.errorCode = errorCode;
	}

	/**
	 * What changed, for the caller to recognize the refusal by.
	 *
	 * @return the error code, such as {@code SMSS_CHANGED}
	 */
	public String getErrorCode() {
		return this.errorCode;
	}
}
