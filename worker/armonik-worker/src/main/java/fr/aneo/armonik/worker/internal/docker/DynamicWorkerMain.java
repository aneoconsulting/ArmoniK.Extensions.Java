/*
 * Copyright © 2025 ANEO (armonik@aneo.fr)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package fr.aneo.armonik.worker.internal.docker;

import fr.aneo.armonik.worker.ArmoniKWorker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Entry point for the Docker image with dynamic TaskProcessor loading.
 * <p>
 * This class is internal infrastructure code and not part of the public API.
 * It is referenced only by the JAR manifest for Docker image execution.
 * </p>
 */
final class DynamicWorkerMain {
  private static final Logger logger = LoggerFactory.getLogger(DynamicWorkerMain.class);

  private DynamicWorkerMain() {
  }

  public static void main(String[] args) {
    logger.info("Starting ArmoniK Worker in dynamic loading mode");

    try {
      var worker = ArmoniKWorker.withDynamicLoading();
      worker.start();

      logger.info("Worker started successfully. Waiting for tasks...");
      worker.blockUntilShutdown();

    } catch (Exception e) {
      logger.error("Worker failed to start", e);
      System.exit(1);
    }
  }
}
