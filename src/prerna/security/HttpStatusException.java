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
package prerna.security;

/**
 * An HTTP call that reached the server and got a status outside 200 to 299.
 * {@link HttpHelperUtility} throws it, so a caller can act on the status, such
 * as retrying a 429 or a 503, without reading it out of the message.
 */
public class HttpStatusException extends IllegalArgumentException {

	private static final long serialVersionUID = 1L;

	private final int statusCode;

	/**
	 * A failed HTTP call with the status the server returned.
	 *
	 * @param message    what failed, with the method, URL and status
	 * @param statusCode the HTTP status the server returned
	 */
	public HttpStatusException(String message, int statusCode) {
		super(message);
		this.statusCode = statusCode;
	}

	/**
	 * The HTTP status the server returned.
	 *
	 * @return the status code
	 */
	public int getStatusCode() {
		return this.statusCode;
	}
}
