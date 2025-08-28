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
package prerna.tcp.client;

import prerna.om.Insight;
import prerna.tcp.PayloadStruct;

public class InsightSerializer {

  Insight insight = null;

  public InsightSerializer(Insight insight) {
    this.insight = insight;
  }

  public void serializeInsight(boolean force) {
    // see if the insight has been serialized
    // meh may be even synchronize on insight
    // synchronized(insight) - this will un-necessarily block. will deal when we get to it
    {
      SocketClient sc = (SocketClient) this.insight.getUser().getPythonSocketClient(true);
      if (!this.insight.getSerialized() || this.insight.getContextReinitialized() || force) {
        PayloadStruct ps = new PayloadStruct();
        ps.operation = ps.operation.INSIGHT;

        // set everything from the noun store
        // hopefully this serializes well
        ps.payload = new Object[] {this.insight};
        ps.payloadClasses = new Class[] {this.insight.getClass()};
        ps.hasReturn = false;

        PayloadStruct retStruct = (PayloadStruct) sc.executeCommand(ps);

        this.insight.setSerialized(true);
        this.insight.setContextReinitialized(false);
      }
    }
  }
}
