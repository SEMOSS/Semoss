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
package prerna.remoteviewer.service;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.lang.reflect.Proxy;
import java.util.List;

import org.junit.jupiter.api.Test;

import com.microsoft.playwright.BrowserContext;
import com.microsoft.playwright.Page;

import prerna.reactor.playwright.PlaywrightSession;
import prerna.reactor.playwright.PlaywrightStep;
import prerna.reactor.playwright.Selector;
import prerna.remoteviewer.model.RemoteBrowserInputEvent;

class RemoteBrowserRecordingServiceTest {

	@Test
	void fillEventsForDifferentSelectorsCreateDifferentTypeSteps() {
		RemoteBrowserSession session = session();

		RemoteBrowserRecordingService.record(session, fill("input[name=monday]", "8"));
		RemoteBrowserRecordingService.record(session, fill("input[name=tuesday]", "7"));

		List<PlaywrightStep> steps = session.getRecordingHistory().steps().get("tab-1").get(0);
		assertEquals(2, steps.size());
		assertEquals("8", steps.get(0).text());
		assertEquals("input[name=monday]", steps.get(0).selector().value());
		assertEquals("7", steps.get(1).text());
		assertEquals("input[name=tuesday]", steps.get(1).selector().value());
	}

	@Test
	void repeatedAtomicFillReplacesRatherThanAppendsRecordedValue() {
		RemoteBrowserSession session = session();

		RemoteBrowserRecordingService.record(session, fill("input[name=search]", "java"));
		RemoteBrowserRecordingService.record(session, fill("input[name=search]", "playwright"));

		List<PlaywrightStep> steps = session.getRecordingHistory().steps().get("tab-1").get(0);
		assertEquals(1, steps.size());
		assertEquals("playwright", steps.get(0).text());
	}

	private static RemoteBrowserSession session() {
		BrowserContext context = noOpProxy(BrowserContext.class);
		Page page = noOpProxy(Page.class);
		PlaywrightSession playwrightSession = PlaywrightSession.forRemoteViewer(context, page, 120);
		RemoteBrowserSession session = new RemoteBrowserSession("session", "user", playwrightSession, 1280, 800);
		session.setRecordingEnabled(true);
		return session;
	}

	@SuppressWarnings("unchecked")
	private static <T> T noOpProxy(Class<T> type) {
		return (T) Proxy.newProxyInstance(type.getClassLoader(), new Class<?>[] { type }, (proxy, method, args) -> {
			Class<?> returnType = method.getReturnType();
			if (!returnType.isPrimitive()) {
				return null;
			}
			if (returnType == boolean.class) {
				return false;
			}
			if (returnType == char.class) {
				return '\0';
			}
			return 0;
		});
	}

	private static RemoteBrowserInputEvent fill(String selector, String value) {
		RemoteBrowserInputEvent event = new RemoteBrowserInputEvent();
		event.setType("fill-element");
		event.setText(value);
		event.setSelector(new Selector("css", selector, null));
		event.setLabel(selector);
		event.setTag("input");
		return event;
	}
}
