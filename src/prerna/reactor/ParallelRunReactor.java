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
package prerna.reactor;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;

public class ParallelRunReactor extends AbstractReactor {

  private static final Logger classLogger = LogManager.getLogger(ParallelRunReactor.class);

  public ParallelRunReactor() {
    this.keysToGet = new String[] {ReactorKeysEnum.PARALLEL_WORKER.getKey()};
  }

  @Override
  public NounMetadata execute() {
    organizeKeys();
    String className = keyValue.get(keysToGet[0]);
    if (className == null) {
      throw new SemossPixelException(getError("No worker defined"));
    }

    NounMetadata noun =
        new NounMetadata(
            "Staring job in parallel", PixelDataType.CONST_STRING, PixelOperationType.OPERATION);
    try {
      Object opw = Class.forName(className).newInstance();
      if (opw == null || !(opw instanceof IParallelWorker)) {
        throw new SemossPixelException(getError("Worker must be IParallelWorker"));
      }

      // execute
      IParallelWorker pw = (IParallelWorker) opw;
      pw.setInisight(insight);
      ParallelThread pt = new ParallelThread();
      pt.worker = pw;
      java.lang.Thread t = new Thread(pt);
      t.start();

    } catch (InstantiationException e) {
      classLogger.error(Constants.STACKTRACE, e);
      throw new SemossPixelException(getError("Cannot Instantiate class " + className));
    } catch (IllegalAccessException e) {
      classLogger.error(Constants.STACKTRACE, e);
      throw new SemossPixelException(getError("Illegal Access class " + className));
    } catch (ClassNotFoundException e) {
      classLogger.error(Constants.STACKTRACE, e);
      throw new SemossPixelException(getError("Not Found  class " + className));
    }

    return noun;
  }
}
