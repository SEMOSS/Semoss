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
package prerna.testing;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ApiSemossTestSetupUtils {

  public static void setup(boolean parallel) throws Exception {
    if (parallel) {
      setupParallel();
    } else {
      setupSeq();
    }
  }

  private static void setupParallel() throws Exception {
    // TODO Auto-generated method stub
    List<Callable<Void>> tasks = getTasks();
    ExecutorService es = Executors.newCachedThreadPool();
    try {
      es.invokeAll(tasks);
    } catch (Exception e) {
      e.printStackTrace();
      throw e;
    } finally {
      es.shutdown();
    }
  }

  private static void setupSeq() throws Exception {
    for (Callable<Void> t : getTasks()) {
      t.call();
    }
  }

  private static List<Callable<Void>> getTasks() {
    List<Callable<Void>> tasks = new ArrayList<>();
    ApiSemossTestEmailUtils.addStartupTasks(tasks);
    ApiSemossTestEngineUtils.addDBStartupTasks(tasks);
    return tasks;
  }

  public static void ensureTestFolderStructure() throws IOException {
    String testFolderBase = ApiTestsSemossConstants.TEST_BASE_DIRECTORY;
    Path project = Paths.get(testFolderBase, "project");
    Path function = Paths.get(testFolderBase, "function");
    Path model = Paths.get(testFolderBase, "model");
    Path storage = Paths.get(testFolderBase, "storage");
    Path vector = Paths.get(testFolderBase, "vector");
    Path venv = Paths.get(testFolderBase, "venv");

    List<Path> ps = new ArrayList<>();
    ps.add(project);
    ps.add(function);
    ps.add(model);
    ps.add(storage);
    ps.add(vector);
    ps.add(venv);

    for (Path p : ps) {
      if (Files.notExists(p) && !Files.isDirectory(p)) {
        Files.createDirectories(p);
      }
    }
  }
}
