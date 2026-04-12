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
package prerna.project.impl;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import jakarta.mail.Session;
import jakarta.mail.Store;
import prerna.util.Constants;
import prerna.util.SocialPropertiesProcessor;

public class ProjectProperties {

	private static final Logger classLogger = LogManager.getLogger(ProjectProperties.class);
	private static final String ADMIN_DIRECTORY = ".admin";

	private String projectDirString = null;
	private File adminDir = null;
	private File socialProp = null;
	private SocialPropertiesProcessor processor = null;

	public ProjectProperties(String projectDirString, String projectName, String projectId) {
		this.projectDirString = projectDirString;
		this.adminDir = new File(this.projectDirString + "/" + ADMIN_DIRECTORY);
		if (!this.adminDir.exists() || !this.adminDir.isDirectory()) {
			this.adminDir.mkdirs();
		}

		String socialPropertiesFileLoc = this.adminDir.getAbsolutePath() + "/" + Constants.SOCIAL_PROPERTIES_FILENAME;
		this.socialProp = new File(socialPropertiesFileLoc);
		if (!this.socialProp.exists() || !this.socialProp.isFile()) {
			try {
				this.socialProp.createNewFile();
			} catch (IOException e) {
				classLogger.error(Constants.STACKTRACE, e);
			}
		}
		this.processor = new SocialPropertiesProcessor(socialPropertiesFileLoc);
	}

	public void updateProviderProperties(String provider, Map<String, String> mods) throws Exception {
		this.processor.updateProviderProperties(provider, mods);
	}

	public void updateAllProperties(Map<String, String> mods) throws Exception {
		this.processor.updateAllProperties(mods);
	}

	public Map<String, Boolean> getLoginsAllowed() {
		return this.processor.getLoginsAllowed();
	}

	public String getProperty(String key) {
		return this.processor.getProperty(key);
	}

	public Object get(Object key) {
		return this.processor.get(key);
	}

	public boolean containsKey(String key) {
		return this.processor.containsKey(key);
	}

	public Set<String> stringPropertyNames() {
		return this.processor.stringPropertyNames();
	}

	public Map<String, String[]> getSamlAttributeNames() {
		return this.processor.getSamlAttributeNames();
	}

	public boolean emailEnabled() {
		return this.processor.smtpEmailEnabled();
	}

	public boolean pop3EmailEnabled() {
		return this.processor.pop3EmailEnabled();
	}

	public boolean imapEmailEnabled() {
		return this.processor.imapEmailEnabled();
	}

	public String getSmtpSender() {
		return this.processor.getSmtpSender();
	}

	@Deprecated
	public Session getEmailSession() {
		classLogger.warn("METHOD DEPRECATED - PLEASE USE getSmtpEmailSession()");
		classLogger.warn("METHOD DEPRECATED - PLEASE USE getSmtpEmailSession()");
		classLogger.warn("METHOD DEPRECATED - PLEASE USE getSmtpEmailSession()");
		classLogger.warn("METHOD DEPRECATED - PLEASE USE getSmtpEmailSession()");
		return getSmtpEmailSession();
	}

	public Session getSmtpEmailSession() {
		return this.processor.getSmtpEmailSession();
	}

	public Store getPop3EmailStore() {
		return this.processor.getPop3EmailStore();
	}

	public Store getImapEmailStore() {
		return this.processor.getImapEmailStore();
	}

	public Map<String, String> getEmailStaticProps() {
		return this.processor.getSmtpEmailStaticProps();
	}

	public void reloadProps() {
		this.processor.reloadProps();
	}

	public File getSocialProp() {
		return socialProp;
	}

}
