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
package fr.aneo.armonik.client;

import fr.aneo.armonik.client.blob.BlobService;
import fr.aneo.armonik.client.blob.event.BlobCompletionEventWatcher;
import fr.aneo.armonik.client.session.SessionService;
import fr.aneo.armonik.client.task.TaskService;

/**
 * Facade interface providing access to all ArmoniK client service components.
 * <p>
 * The {@code Services} interface serves as the primary access point for advanced ArmoniK operations,
 * exposing individual service facades that wrap the underlying gRPC communication with the ArmoniK
 * Control Plane. This design enables both high-level convenience through {@link fr.aneo.armonik.client.ArmoniKClient}
 * and fine-grained control for advanced use cases.
 *
 * <h2>Service Components</h2>
 * <p>
 * Each service component provides specialized functionality for different aspects of ArmoniK interaction:
 * <ul>
 *   <li><strong>{@link SessionService}:</strong> Session lifecycle management and configuration</li>
 *   <li><strong>{@link BlobService}:</strong> Blob (data) upload, download, and metadata operations</li>
 *   <li><strong>{@link TaskService}:</strong> Task submission and execution management</li>
 *   <li><strong>{@link BlobCompletionEventWatcher}:</strong> Real-time event streaming for blob state changes</li>
 * </ul>
 *
 * @see fr.aneo.armonik.client.ArmoniKClient#services()
 * @see SessionService
 * @see BlobService
 * @see TaskService
 * @see BlobCompletionEventWatcher
 * @see fr.aneo.armonik.client.ArmoniKClient
 */

public interface Services {

  SessionService sessions();

  BlobService blobs();

  TaskService tasks();

  BlobCompletionEventWatcher blobCompletionEventWatcher();
}
