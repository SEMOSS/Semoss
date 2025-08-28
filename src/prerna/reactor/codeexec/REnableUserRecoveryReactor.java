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
package prerna.reactor.codeexec;

import prerna.auth.User;
import prerna.engine.impl.r.IRUserConnection;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class REnableUserRecoveryReactor extends AbstractReactor {

  public REnableUserRecoveryReactor() {
    this.keysToGet = new String[] {ReactorKeysEnum.ENABLE.getKey()};
  }

  @Override
  public NounMetadata execute() {
    this.organizeKeys();

    User user = this.insight.getUser();
    if (user == null) throw new IllegalArgumentException("User is not defined.");
    IRUserConnection rcon = user.getRcon();
    if (rcon == null) throw new IllegalArgumentException("The user's R connection is not defined.");

    String enableString = this.keyValue.get(this.keysToGet[0]);
    if (enableString != null) {
      rcon.setRecoveryEnabled(Boolean.parseBoolean(enableString));
    }

    return new NounMetadata(rcon.isRecoveryEnabled(), PixelDataType.BOOLEAN);
  }
}
