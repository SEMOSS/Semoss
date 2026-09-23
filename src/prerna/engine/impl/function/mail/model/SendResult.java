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
package prerna.engine.impl.function.mail.model;

/**
 * What happened to one send.
 *
 * <p>
 * The sender is reported back because it is not always the one that was asked
 * for. Graph sends as the mailbox it posts against, so an engine that was given
 * a different from address sends as its own and says so here rather than
 * leaving the caller to assume otherwise.
 *
 * @param delivered    whether the mail server accepted it
 * @param actualSender the address it actually went out as
 */
public record SendResult(boolean delivered, String actualSender) {

	/**
	 * @param actualSender the address it went out as
	 * @return a result saying the mail server took it
	 */
	public static SendResult delivered(String actualSender) {
		return new SendResult(true, actualSender);
	}

	/**
	 * @param actualSender the address it would have gone out as
	 * @return a result saying it did not go
	 */
	public static SendResult failed(String actualSender) {
		return new SendResult(false, actualSender);
	}
}
