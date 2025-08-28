/***************************************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
 *
 * If your use of this software does not include any GPLv2 components: Licensed under the Apache
 * License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 ***************************************************************************************************/
package prerna.reactor.frame.gaas.processors;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.stream.Collectors;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import prerna.engine.impl.vector.VectorDatabaseCSVWriter;
import prerna.util.Constants;

public class TextFileProcessor extends AbstractFileProcessor {

  private static final Logger classLogger = LogManager.getLogger(PPTProcessor.class);

  public TextFileProcessor(String filePath, VectorDatabaseCSVWriter writer) {
    super(filePath, writer);
  }

  @Override
  public void process() throws IOException {
    String source = getSource(this.filePath);

    String fileContent = null;
    try (BufferedReader reader = new BufferedReader(new FileReader(this.filePath))) {
      fileContent = reader.lines().collect(Collectors.joining("\n"));
    } catch (IOException e) {
      classLogger.error(Constants.STACKTRACE, e);
      throw e;
    }

    // for a text document there is only ever one page / divider
    String pageIndex = "1";
    this.writer.writeRow(source, pageIndex, fileContent);
  }
}
