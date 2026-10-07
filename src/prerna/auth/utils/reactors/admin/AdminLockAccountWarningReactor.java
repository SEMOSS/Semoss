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
package prerna.auth.utils.reactors.admin;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.PasswordRequirements;
import prerna.auth.User;
import prerna.auth.utils.SecurityAdminUtils;
import prerna.engine.impl.function.mail.engine.SMTPFunctionEngine;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.EmailUtility;
import prerna.util.SocialPropertiesUtil;
import prerna.util.Utility;

public class AdminLockAccountWarningReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(AdminLockAccountWarningReactor.class);
	private static final String ACCOUNT_LOCK_WARNING_TEMPLATE = "accountLockWarning.html";

	@Override
	public NounMetadata execute() {
		User user = this.insight.getUser();
		SecurityAdminUtils adminUtils = SecurityAdminUtils.getInstance(user);
		if (adminUtils == null) {
			throw new IllegalArgumentException("User must be an admin to perform this function");
		}

		int daysToLock = -1;
		try {
			daysToLock = PasswordRequirements.getInstance().getDaysToLock();
		} catch (Exception e) {
			classLogger.error("Unable to send account-lock warning notifications.", e);
		}

		if (daysToLock < 0) {
			throw new IllegalArgumentException("No value set to lock accounts");
		}

		List<String> emailsSentTo = new ArrayList<>();

		List<Object[]> listToEmail = adminUtils.getUserEmailsGettingLocked();
		if (!listToEmail.isEmpty()) {
			// looked up only once there is somebody to warn, so a mail server is not
			// required to find out that nobody is about to be locked out
			SMTPFunctionEngine mailEngine = SocialPropertiesUtil.getInstance().getSmtpEngine();
			if (mailEngine == null) {
				throw new IllegalArgumentException("Need to define an smtp server to send the account lock warnings");
			}

			final String DAYS_SINCE_LAST_LOGIN_REPLACEMENT = "$daysSinceLastLogin$";
			final String DAYS_TO_LOCK_REPLACEMENT = "$daysToLock$";

			String template = null;
			String templatePath = Utility.getDIHelperProperty(Constants.EMAIL_TEMPLATES);
			if (templatePath.endsWith("\\") || templatePath.endsWith("/")) {
				templatePath += ACCOUNT_LOCK_WARNING_TEMPLATE;
			} else {
				templatePath += "/" + ACCOUNT_LOCK_WARNING_TEMPLATE;
			}
			File templateFile = new File(templatePath);
			if (templateFile.exists() && templateFile.isFile()) {
				try {
					template = FileUtils.readFileToString(templateFile, StandardCharsets.UTF_8);
				} catch (IOException e) {
					classLogger.error("Unable to send account-lock warning notifications.", e);
					classLogger.info("Using default account lock warning text");
					template = "<html><p>Our records show you have not logged into the SEMOSS application for "
							+ DAYS_SINCE_LAST_LOGIN_REPLACEMENT + " days. "
							+ "Your account will be locked if the number of days exceeds " + DAYS_TO_LOCK_REPLACEMENT
							+ " days. "
							+ "If you no longer need access to the application, please ignore this email.</p></html>";
				}
			} else {
				template = "<html><p>Our records show you have not logged into the SEMOSS application for "
						+ DAYS_SINCE_LAST_LOGIN_REPLACEMENT + " days. "
						+ "Your account will be locked if the number of days exceeds " + DAYS_TO_LOCK_REPLACEMENT
						+ " days. "
						+ "If you no longer need access to the application, please ignore this email.</p></html>";
			}
			for (Object[] emailInfo : listToEmail) {
				String email = (String) emailInfo[0];
				long daysSinceLastLogin = ((Number) emailInfo[1]).longValue();

				Map<String, String> emailReplacements = SocialPropertiesUtil.getInstance().getEmailStaticProps();
				emailReplacements.put(DAYS_TO_LOCK_REPLACEMENT, daysToLock + "");
				emailReplacements.put(DAYS_SINCE_LAST_LOGIN_REPLACEMENT, daysSinceLastLogin + "");
				String message = EmailUtility.fillEmailComponents(template, emailReplacements);

				mailEngine.sendEmail(new String[] { email }, null, null,
						SocialPropertiesUtil.getInstance().getSmtpSender(), "WARNING! Account Locking Soon", message,
						true, null);

				emailsSentTo.add(email);
			}
		}

		NounMetadata noun = new NounMetadata(emailsSentTo, PixelDataType.CONST_STRING);
		noun.addAdditionalReturn(getSuccess("Emails sent to " + emailsSentTo.size() + " users"));
		return noun;
	}

}
