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
package prerna.engine.impl.function.mail.engine;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.engine.impl.function.AbstractFunctionEngine;
import prerna.engine.impl.function.FunctionParameter;
import prerna.engine.impl.function.mail.config.MailProperties;
import prerna.engine.impl.function.mail.model.OutboundMail;
import prerna.engine.impl.function.mail.model.SendResult;
import prerna.engine.impl.function.mail.policy.SendMailPolicy;
import prerna.om.Insight;
import prerna.util.Constants;

/**
 * What a sending mail engine looks like from outside.
 *
 * <p>
 * This is the only part that knows it is a function engine. It publishes the
 * parameters a caller sees, takes the map of values they passed, and answers
 * with a map. What may be sent is settled by a {@link SendMailPolicy}, and how
 * it leaves is a sender - neither of which knows anything about engines or
 * catalogs.
 *
 * <p>
 * The published parameters are built from the policy rather than fixed, so an
 * engine describes itself as it is actually configured. One with a pinned
 * sender does not offer a {@code from} parameter at all; one with default
 * recipients stops requiring {@code to}; one that allows attachments gains a
 * parameter for them. That matters most for a model calling the function, which
 * has only these descriptions to go on and would otherwise pass something the
 * engine is going to refuse.
 */
public abstract class AbstractSendMailFunctionEngine extends AbstractFunctionEngine {

	private static final Logger classLogger = LogManager.getLogger(AbstractSendMailFunctionEngine.class);

	public static final String SMTP_SENDER_KEY = MailProperties.SMTP_SENDER;
	public static final String SMTP_SENDER_NAME_KEY = MailProperties.SMTP_SENDER_NAME;
	public static final String ALLOW_SENDER_OVERRIDE_KEY = MailProperties.ALLOW_SENDER_OVERRIDE;
	public static final String ALLOWED_RECIPIENT_DOMAINS_KEY = MailProperties.ALLOWED_RECIPIENT_DOMAINS;
	public static final String DEFAULT_TO_KEY = MailProperties.DEFAULT_TO;
	public static final String DEFAULT_CC_KEY = MailProperties.DEFAULT_CC;
	public static final String DEFAULT_BCC_KEY = MailProperties.DEFAULT_BCC;
	public static final String SUBJECT_PREFIX_KEY = MailProperties.SUBJECT_PREFIX;
	public static final String HTML_KEY = MailProperties.HTML;
	public static final String MAX_RECIPIENTS_KEY = MailProperties.MAX_RECIPIENTS;
	public static final String ALLOW_ATTACHMENTS_KEY = MailProperties.ALLOW_ATTACHMENTS;

	private static final String TO_PARAM = "to";
	private static final String CC_PARAM = "cc";
	private static final String BCC_PARAM = "bcc";
	private static final String SUBJECT_PARAM = "subject";
	private static final String MESSAGE_PARAM = "message";
	private static final String HTML_PARAM = "html";
	private static final String FROM_PARAM = "from";
	private static final String ATTACHMENTS_PARAM = "attachments";

	private SendMailPolicy sendPolicy;

	@Override
	public void open(Properties smssProp) throws Exception {
		super.open(smssProp);
		this.sendPolicy = SendMailPolicy.from(smssProp);
		setDefaultFunctionMetadata();
	}

	/**
	 * Send one email.
	 *
	 * @param parameterValues the runtime parameters for this call
	 * @return what was sent, which reports the address it actually went out as
	 * @throws IllegalArgumentException when the call asks for something this engine
	 *                                  does not permit, or the mail server refuses
	 *                                  it
	 */
	@Override
	public Object execute(Map<String, Object> parameterValues) {
		// Insight is framework context, not a function argument. Work on a copy so an
		// invocation never removes it from the caller's map.
		Map<String, Object> parameters = parameterValues == null ? new HashMap<>() : new HashMap<>(parameterValues);
		Insight executingInsight = (Insight) parameters.remove(Constants.INSIGHT);
		validateRequiredParameters(parameters);

		OutboundMail message = this.sendPolicy.prepare(parameters, executingInsight);
		classLogger.info("Sending an email from {} to {} recipient(s) through {}", message.from(),
				message.recipientCount(), getSendDescription());

		SendResult result = sendMessage(message);
		if (!result.delivered()) {
			throw new IllegalArgumentException(getSendFailureMessage());
		}

		Map<String, Object> output = new LinkedHashMap<>();
		output.put("success", true);
		output.put("from", result.actualSender() == null ? message.from() : result.actualSender());
		output.put("to", message.to());
		output.put("cc", message.cc());
		output.put("bcc", message.bcc());
		output.put("subject", message.subject());
		if (!message.attachments().isEmpty()) {
			output.put("attachments", message.attachments().stream().map(path -> new File(path).getName()).toList());
		}
		return output;
	}

	/**
	 * Hand a checked message to whatever this engine sends through.
	 *
	 * @param message the message, already allowed by the policy
	 * @return whether it was delivered, and the address it went out as
	 */
	protected abstract SendResult sendMessage(OutboundMail message);

	/**
	 * @return what to tell a caller when the mail server would not take the
	 *         message, which a subclass can make more specific to what it sends
	 *         through
	 */
	protected String getSendFailureMessage() {
		return "The email was not sent. Check the smtp server settings on this function engine and the logs for details";
	}

	/**
	 * Fill in the name and description an engine that is not in the catalog has no
	 * one to give it.
	 *
	 * @param engineId           the id to open under
	 * @param props              the settings the caller supplied
	 * @param defaultDescription what to describe the function as
	 * @return the settings, ready to open with
	 */
	protected static Properties transientProperties(String engineId, Properties props, String defaultDescription) {
		return MailProperties.transientProperties(engineId, props, defaultDescription, NAME_KEY, DESCRIPTION_KEY);
	}

	/**
	 * Describe this engine the way it is actually configured, unless whoever
	 * cataloged it wrote their own description and parameters.
	 *
	 * <p>
	 * Everything here checks for what the SMSS already says before filling it in,
	 * so a hand written function definition is never overwritten by a generated
	 * one.
	 */
	protected void setDefaultFunctionMetadata() {
		if (this.functionDescription == null || this.functionDescription.isEmpty()) {
			this.functionDescription = getDefaultFunctionDescription();
		}
		if (this.parameters == null || this.parameters.isEmpty()) {
			List<FunctionParameter> defaults = new ArrayList<>();
			defaults.add(new FunctionParameter(TO_PARAM, "string", "Comma separated list of recipient email addresses."
					+ defaultText(joinList(this.sendPolicy.defaultTo())) + allowedDomainText()));
			defaults.add(new FunctionParameter(CC_PARAM, "string", "Optional comma separated list of addresses to copy."
					+ defaultText(joinList(this.sendPolicy.defaultCc()))));
			defaults.add(new FunctionParameter(BCC_PARAM, "string",
					"Optional comma separated list of addresses to blind copy."
							+ defaultText(joinList(this.sendPolicy.defaultBcc()))));
			defaults.add(new FunctionParameter(SUBJECT_PARAM, "string",
					"The subject line. Keep it short and specific to what the message is about."));
			defaults.add(new FunctionParameter(MESSAGE_PARAM, "string",
					"The body of the email. Write the full message, not a summary of it."));
			defaults.add(new FunctionParameter(HTML_PARAM, "boolean",
					"Optional. Set to true when the body is html rather than plain text. Defaults to "
							+ this.sendPolicy.html() + "."));
			if (this.sendPolicy.sender() == null) {
				defaults.add(new FunctionParameter(FROM_PARAM, "string",
						"The sender address. This mail server does not have one configured, so every email has to say who it is from."));
			} else if (this.sendPolicy.allowSenderOverride()) {
				defaults.add(new FunctionParameter(FROM_PARAM, "string",
						"Optional sender address." + defaultText(this.sendPolicy.sender())));
			}
			if (this.sendPolicy.allowAttachments()) {
				defaults.add(new FunctionParameter(ATTACHMENTS_PARAM, "string",
						"Optional comma separated list of file names to attach. Each must already exist in the files of the insight making this call."));
			}
			this.parameters = defaults;
		}

		if (this.requiredParameters == null || this.requiredParameters.isEmpty()) {
			List<String> required = new ArrayList<>(Arrays.asList(SUBJECT_PARAM, MESSAGE_PARAM));
			if (this.sendPolicy.defaultTo().isEmpty() && this.sendPolicy.defaultCc().isEmpty()
					&& this.sendPolicy.defaultBcc().isEmpty()) {
				required.add(0, TO_PARAM);
			}
			if (this.sendPolicy.sender() == null) {
				required.add(FROM_PARAM);
			}
			this.requiredParameters = required;
		}
	}

	/**
	 * @return a sentence naming the domains this engine will send to, so a caller
	 *         reading the parameter knows before trying, or empty when any
	 *         recipient is allowed
	 */
	private String allowedDomainText() {
		if (this.sendPolicy.allowedRecipientDomains().isEmpty()) {
			return "";
		}
		return " Only addresses in these domains can be used: "
				+ String.join(", ", this.sendPolicy.allowedRecipientDomains()) + ".";
	}

	/**
	 * @param address the value to check
	 * @param source  where it came from, named in the error
	 * @throws IllegalArgumentException when it is not an email address
	 */
	protected static void validateEmailAddress(String address, String source) {
		SendMailPolicy.validateEmailAddress(address, source);
	}

	/**
	 * @param value a comma or semicolon separated list, which may be null
	 * @return the entries, empty when there are none
	 */
	protected static List<String> splitList(String value) {
		return MailProperties.splitList(value);
	}

	/**
	 * @param values the entries, which may be null or empty
	 * @return them joined for a description, or null when there are none to mention
	 */
	protected static String joinList(List<String> values) {
		return values == null || values.isEmpty() ? null : String.join(", ", values);
	}

	/**
	 * @param value        the configured value
	 * @param defaultValue what it is when nothing was configured
	 * @return the value
	 */
	protected static boolean parseBoolean(String value, boolean defaultValue) {
		return MailProperties.parseBoolean(value, defaultValue);
	}

	/**
	 * @param recipients the addresses, which may be null or empty
	 * @return them as an array, or null when there are none, which is what the mail
	 *         libraries read as "none"
	 */
	protected static String[] toArray(List<String> recipients) {
		return recipients == null || recipients.isEmpty() ? null : recipients.toArray(String[]::new);
	}

	/**
	 * @param value the value to trim
	 * @return it without surrounding space, or null when there is nothing left
	 */
	protected static String trimToNull(String value) {
		return MailProperties.trimToNull(value);
	}

	/**
	 * @return how to describe this function when the SMSS does not, which is what a
	 *         model calling it reads to decide whether it is the right one
	 */
	protected abstract String getDefaultFunctionDescription();

	/**
	 * @return what this engine sends through, for the line logged on every send
	 */
	protected abstract String getSendDescription();
}
