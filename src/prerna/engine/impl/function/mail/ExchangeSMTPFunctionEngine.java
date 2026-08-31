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
package prerna.engine.impl.function.mail;

import java.util.Properties;

import prerna.engine.api.FunctionTypeEnum;

/**
 * Function engine that sends email as a Microsoft 365 mailbox.
 *
 * <p>
 * The same engine as {@link SMTPFunctionEngine}, with the same guardrails,
 * pointed at Microsoft 365 and defaulting the other way on
 * {@link SMTPFunctionEngine#MAIL_TRANSPORT_KEY}:
 *
 * <ul>
 * <li>{@code graph}, the default, posts the message to the Microsoft Graph
 * {@code sendMail} endpoint. It needs the {@code Mail.Send} application
 * permission and nothing else.</li>
 * <li>{@code jakarta} hands the message to {@code smtp.office365.com} with the
 * token presented through XOAUTH2. That additionally needs the
 * {@code SMTP.SendAsApp} permission, a service principal, a mailbox grant, and
 * SMTP AUTH enabled for both the tenant and the mailbox.</li>
 * </ul>
 * 
 * <p>
 * Graph is the default because it is the one that keeps working: Microsoft has
 * spent years restricting the protocol endpoints, and every restriction lands
 * on the second option rather than the first. The app registration is
 * configured the same way for both, so switching is one setting.
 */
public class ExchangeSMTPFunctionEngine extends SMTPFunctionEngine {

	/**
	 * Build an Exchange send engine that is not in the catalog, for a caller that
	 * already holds an app registration and wants this engine's send handling
	 * rather than its own.
	 *
	 * @param engineId the id to open under, used only for logging
	 * @param props    the mailbox and app registration properties
	 * @return the opened engine
	 * @throws Exception when the properties do not describe a usable mailbox
	 */
	public static ExchangeSMTPFunctionEngine openTransientEngine(String engineId, Properties props) throws Exception {
		ExchangeSMTPFunctionEngine engine = new ExchangeSMTPFunctionEngine();
		// no folder structure, no secret store lookup, no catalog entry
		engine.setBasic(true);
		engine.open(transientProperties(engineId, props, "Send an email as the " + engineId + " mailbox"));
		return engine;
	}

	@Override
	protected String getDefaultTransport() {
		return GRAPH_TRANSPORT;
	}

	@Override
	protected String getDefaultFunctionDescription() {
		return """
				Send an email from a Microsoft 365 mailbox. Use this to notify someone of a result, deliver a \
				summary, or route a request onward. The message is sent immediately and cannot be recalled, so \
				confirm the recipients and the wording before calling this.\
				""";
	}

	@Override
	public String getCatalogSubType(Properties smssProp) {
		return FunctionTypeEnum.EXCHANGE_SMTP.name();
	}

}
