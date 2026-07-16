/*******************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
 * Licensed under the Apache License, Version 2.0 (the "License");
 *******************************************************************************/
package prerna.remoteviewer.security;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

import prerna.remoteviewer.model.RemoteBrowserInputEvent;

class RemoteBrowserInputEventValidatorTest {

	@Test
	void contextSnapshotRequiresCorrelationId() {
		RemoteBrowserInputEvent event = new RemoteBrowserInputEvent();
		event.setType("context-snapshot");
		assertThrows(IllegalArgumentException.class, () -> RemoteBrowserInputEventValidator.validate(event, 100, 100));

		event.setRequestId("snapshot-1");
		assertDoesNotThrow(() -> RemoteBrowserInputEventValidator.validate(event, 100, 100));
	}
}
