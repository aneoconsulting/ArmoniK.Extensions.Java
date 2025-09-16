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
import fr.aneo.armonik.client.session.SessionService;
import fr.aneo.armonik.client.task.TaskService;

/**
 * Container interface exposing the ArmoniK service facades.
 */
public interface Services {

  SessionService sessions();

  BlobService blobs();

  TaskService tasks();
}
