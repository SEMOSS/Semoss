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
package prerna.sablecc2;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.junit.jupiter.api.Test;
import prerna.auth.User;
import prerna.engine.api.IHeadersDataRow;
import prerna.engine.impl.usage.UsageLimitException;
import prerna.engine.impl.usage.UsageRule.Metric;
import prerna.om.Insight;
import prerna.om.Pixel;
import prerna.om.ThreadStore;
import prerna.reactor.export.IFormatter;
import prerna.sablecc2.comm.PixelJobRunner;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.om.task.ITask;

class PixelStreamUsageLimitsUnitTests {
	private record Fixture(PixelRunner runner, Pixel pixel, ITask task, User actor, PixelJobRunner job) {
	}

	private static Fixture fixture() {
		var actor = mock(User.class);
		var insight = mock(Insight.class);
		when(insight.getUser()).thenReturn(actor);
		when(insight.getInsightId()).thenReturn("insight");
		when(insight.getDelayedMessages()).thenReturn(List.of());
		var task = mock(ITask.class);
		when(task.getNumCollect()).thenReturn(-1);
		when(task.getId()).thenReturn("task");
		when(task.getPragma("xCache")).thenReturn("False");
		when(task.getMetaMap()).thenReturn(Map.of());
		var format = mock(IFormatter.class);
		when(format.getFormatType()).thenReturn("TABLE");
		when(task.getFormatter()).thenReturn(format);
		var row = mock(IHeadersDataRow.class);
		when(row.getHeaders()).thenReturn(new String[] { "value" });
		when(row.getRawHeaders()).thenReturn(new String[] { "value" });
		when(row.getValues()).thenReturn(new Object[] { 1 });
		when(task.next()).thenReturn(row);
		var runner = mock(PixelRunner.class);
		when(runner.getInsight()).thenReturn(insight);
		var pixel = new Pixel("pixel", "Select();");
		when(runner.getResults())
				.thenReturn(new ArrayList<>(List.of(new NounMetadata(task, PixelDataType.FORMATTED_DATA_SET))));
		when(runner.getReturnPixelList()).thenReturn(new ArrayList<>(List.of(pixel)));
		var job = mock(PixelJobRunner.class);
		when(job.getInsight()).thenReturn(insight);
		return new Fixture(runner, pixel, task, actor, job);
	}

	private static JsonObject stream(Fixture fixture) throws Exception {
		var previous = ThreadStore.getUser();
		var parent = mock(User.class);
		ThreadStore.setUser(parent);
		var output = new ByteArrayOutputStream();
		var failure = new AtomicReference<Throwable>();
		var cleared = new AtomicBoolean();
		try {
			var stream = PixelStreamUtility.collectPixelData(fixture.runner, fixture.job,
					() -> cleared.set(ThreadStore.getUser() == null));
			var thread = Thread.ofVirtual().start(() -> {
				try {
					stream.write(output);
				} catch (Throwable e) {
					failure.set(e);
				}
			});
			thread.join(10_000);
			assertFalse(thread.isAlive());
			assertNull(failure.get());
			assertTrue(cleared.get());
			assertSame(parent, ThreadStore.getUser());
			return JsonParser.parseString(output.toString(StandardCharsets.UTF_8)).getAsJsonObject()
					.getAsJsonArray("pixelReturn").get(0).getAsJsonObject();
		} finally {
			ThreadStore.setUser(previous);
		}
	}

	@Test
	void lateRowRefusalProducesValidJsonWithAnExplicitIncompleteError() throws Exception {
		var fixture = fixture();
		var calls = new AtomicInteger();
		when(fixture.task.hasNext()).thenAnswer(call -> {
			assertSame(fixture.actor, ThreadStore.getUser());
			if (calls.getAndIncrement() == 0)
				return true;
			throw new UsageLimitException(Metric.ROWS, Instant.parse("2026-10-16T00:00:00Z"));
		});
		var result = stream(fixture);
		var output = result.getAsJsonObject("output");
		assertEquals(1, output.getAsJsonObject("data").getAsJsonArray("values").size());
		assertTrue(output.get("incomplete").getAsBoolean());
		assertTrue(output.get("errorMessage").getAsString().contains("ROWS"));
		assertTrue(result.getAsJsonArray("operationType").toString().contains("ERROR"));
		assertTrue(fixture.pixel.isReturnedError());
		verify(fixture.task).close();
	}

	@Test
	void refusalBeforeAnyRowKeepsTheExistingStringErrorShape() throws Exception {
		var fixture = fixture();
		when(fixture.task.hasNext()).thenAnswer(call -> {
			assertSame(fixture.actor, ThreadStore.getUser());
			throw new UsageLimitException(Metric.QUERIES, Instant.parse("2026-10-16T00:00:00Z"));
		});
		var result = stream(fixture);
		assertTrue(result.get("output").getAsString().contains("QUERIES"));
		assertTrue(result.getAsJsonArray("operationType").toString().contains("ERROR"));
		verify(fixture.task).close();
	}

	@Test
	void successfulDeferredReadsKeepTheirNormalDataShape() throws Exception {
		var fixture = fixture();
		var calls = new AtomicInteger();
		when(fixture.task.hasNext()).thenAnswer(call -> {
			assertSame(fixture.actor, ThreadStore.getUser());
			return calls.getAndIncrement() == 0;
		});
		var result = stream(fixture);
		var output = result.getAsJsonObject("output");
		assertEquals(1, output.getAsJsonObject("data").getAsJsonArray("values").size());
		assertFalse(output.has("incomplete"));
		assertFalse(result.getAsJsonArray("operationType").toString().contains("ERROR"));
		assertFalse(fixture.pixel.isReturnedError());
	}

	@Test
	void nonTaskPixelOutputIsUnchanged() throws Exception {
		var fixture = fixture();
		when(fixture.runner.getResults())
				.thenReturn(new ArrayList<>(List.of(new NounMetadata("ok", PixelDataType.CONST_STRING))));
		assertEquals("ok", stream(fixture).get("output").getAsString());
	}
}
