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
package prerna.usertracking.reactors;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;
import java.util.Map;
import org.junit.jupiter.api.Test;
import prerna.auth.User;
import prerna.om.Insight;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.usertracking.ResourceAuditReadService;
import prerna.usertracking.UserAuditTrailUtils;

class ResourceAuditEventsReactorTest {
    @Test
    void bothReactorsReturnMaterializedMapsUsingInsightIdentity() throws Exception {
        User user = mock(User.class);
        Insight insight = mock(Insight.class);
        when(insight.getUser()).thenReturn(user);
        for (AbstractResourceAuditEventsReactor reactor : new AbstractResourceAuditEventsReactor[] {new EngineAuditEventsReactor(), new ProjectAuditEventsReactor()}) {
            reactor.setInsight(insight);
            reactor.getNounStore().makeGenRowStruct("id").add(new NounMetadata("resource", PixelDataType.CONST_STRING));
            reactor.getNounStore().makeGenRowStruct("page").add(new NounMetadata(1, PixelDataType.CONST_INT));
            Map<String,Object> page = Map.of("events", java.util.List.of(), "hasNextPage", false);
            try (var service = mockStatic(ResourceAuditReadService.class)) {
                service.when(() -> ResourceAuditReadService.read(user, reactor.scope(), "resource", Map.of("page", "1"))).thenReturn(page);
                var result = reactor.execute();
                assertEquals(PixelDataType.MAP, result.getNounType());
                assertSame(page, result.getValue());
                service.verify(() -> ResourceAuditReadService.read(user, reactor.scope(), "resource", Map.of("page", "1")));
            }
        }
    }

    @Test
    void databaseErrorsReturnGenericMessageAndAuditFailure() throws Exception {
        User user = mock(User.class);
        Insight insight = mock(Insight.class);
        when(insight.getUser()).thenReturn(user);
        var reactor = new EngineAuditEventsReactor();
        reactor.setInsight(insight);
        reactor.getNounStore().makeGenRowStruct("id").add(new NounMetadata("resource", PixelDataType.CONST_STRING));
        try (var service = mockStatic(ResourceAuditReadService.class); var audit = mockStatic(UserAuditTrailUtils.class)) {
            service.when(() -> ResourceAuditReadService.read(user,"ENGINE","resource",Map.of())).thenThrow(new java.sql.SQLException("private connection information"));
            var exception = assertThrows(IllegalStateException.class, reactor::execute);
            assertEquals("Unable to read the resource audit trail.", exception.getMessage());
            audit.verify(() -> UserAuditTrailUtils.recordEvent(any(UserAuditTrailUtils.AuditEvent.class)));
        }
    }
}
