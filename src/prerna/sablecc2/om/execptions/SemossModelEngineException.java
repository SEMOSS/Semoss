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
package prerna.sablecc2.om.execptions;

import prerna.engine.impl.model.responses.AskErrorModelEngineResponse;
import prerna.engine.impl.model.responses.AskModelEngineResponse;

public class SemossModelEngineException extends RuntimeException {

	private final AskModelEngineResponse<?> errorResponse;

	/**
	 * An error the model provider returned. The message is what the user sees; the
	 * provider's own text stays on the error response.
	 *
	 * @param errorResponse the provider's error, with its text and status code
	 * @param message       the message for the user
	 */
	public SemossModelEngineException(AskModelEngineResponse<?> errorResponse, String message) {
		super(message);
		this.errorResponse = errorResponse;
	}

	public AskModelEngineResponse<?> getErrorResponse() {
		return errorResponse;
	}

	/**
	 * The message a user sees for a provider error: the status code, what it means,
	 * and the reference the full error was logged under. The provider's text is
	 * left out, because it can name accounts, users, resources and the policies
	 * that refused the call.
	 *
	 * @param code      the provider's HTTP status code, or 0 when there is none
	 * @param reason    the reason the client found, such as
	 *                  {@link AskErrorModelEngineResponse#REASON_CONTEXT_OVERFLOW},
	 *                  or null
	 * @param reference the id the full error was logged under
	 * @return the message for the user
	 */
	public static String providerErrorMessage(int code, String reason, String reference) {
		String codePrefix = code > 0 ? "Error code: " + code + ". " : "";
		if (AskErrorModelEngineResponse.REASON_CONTEXT_OVERFLOW.equals(reason)) {
			return codePrefix + "The request is longer than the model can take. Reference: " + reference;
		}
		String meaning = switch (code) {
		case 400 -> "The model provider rejected the request.";
		case 401, 403 -> "The model provider denied access.";
		case 404 -> "The model provider could not find the model.";
		case 408, 504 -> "The model provider timed out. Try again in a moment.";
		case 413 -> "The request is too large for the model provider.";
		case 429 -> "The model provider is over its rate limit. Try again in a moment.";
		default -> code >= 500 && code < 600 ? "The model provider failed. Try again in a moment."
				: "The model provider returned an error.";
		};
		return codePrefix + meaning + " Reference: " + reference;
	}

	// true when this exception (or a cause) is a model error with reason
	// CONTEXT_OVERFLOW
	public static boolean isContextOverflow(Throwable error) {
		return contextOverflowError(error) != null;
	}

	// the CONTEXT_OVERFLOW error details in this exception or its causes, else null
	public static AskErrorModelEngineResponse contextOverflowError(Throwable error) {
		for (Throwable t = error; t != null; t = t.getCause() == t ? null : t.getCause()) {
			if (t instanceof SemossModelEngineException e
					&& e.getErrorResponse() instanceof AskErrorModelEngineResponse details
					&& details.isContextOverflow()) {
				return details;
			}
		}
		return null;
	}
}