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
package prerna.reactor.playwright;

import java.util.UUID;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.microsoft.playwright.Browser;
import com.microsoft.playwright.BrowserContext;
import com.microsoft.playwright.Page;

import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class SessionReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(SessionReactor.class);

	@Override
	public NounMetadata execute() {
		Browser browser = PlaywrightBrowserProvider.getBrowser();

		int width = 1280;
		int height = 800;
		double dpr = 1.0;

		BrowserContext ctx = this.insight.getUser().getSharedPlaywrightContext();
		if (ctx == null) {
			Browser.NewContextOptions ctxOps = new Browser.NewContextOptions().setViewportSize(width, height)
					.setDeviceScaleFactor(dpr).setAcceptDownloads(true);
			ctx = browser.newContext(ctxOps);
			ctx.setDefaultTimeout(60_000);
			ctx.setDefaultNavigationTimeout(60_000);
			this.insight.getUser().setSharedPlaywrightContext(ctx);
		}
		Page page = ctx.newPage();

		PlaywrightSession s = new PlaywrightSession(ctx, page);
		if (s.history.meta() == null) {
			s.history = new StepsEnvelope("1.0", PlaywrightSession.newMeta(""), s.history.steps());
		}

		String id = UUID.randomUUID().toString();
		s.setUserAndSessionId(this.insight.getUser(), id);
		this.insight.getUser().setPlaywrightSession(id, s);

		classLogger.info("Created playwright session successfully with id: {}", id);
		return new NounMetadata(id, PixelDataType.CONST_STRING);
	}

	@Override
	public String getReactorDescription() {
		return "Reactor that initiate a new session and return its id";
	}

}
