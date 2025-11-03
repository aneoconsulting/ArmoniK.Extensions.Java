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
package fr.aneo.armonik.client.definition.blob;

import fr.aneo.armonik.client.definition.TaskDefinition;
import fr.aneo.armonik.client.exception.ArmoniKException;

import java.io.File;

import static java.util.Objects.requireNonNull;
import static java.util.Objects.requireNonNullElse;

/**
 * Definition of input data to be uploaded and associated with tasks in the ArmoniK cluster.
 * <p>
 * An {@code InputBlobDefinition} represents actual data content that will be uploaded to the cluster
 * and made available as input to tasks. It combines metadata (name, deletion policy) with the actual
 * data source encapsulated in a {@link BlobData} instance.
 *
 * <h2>Data Sources</h2>
 * Input blobs support multiple data sources through the {@link BlobData} abstraction:
 * <ul>
 *   <li>{@link InMemoryBlobData}: For in-memory byte arrays (small to medium data)</li>
 *   <li>{@link FileBlobData}: For file-based data (large files, memory efficient)</li>
 *   <li>Custom {@link BlobData} implementations for specialized sources</li>
 * </ul>
 *
 * @see BlobData
 * @see InMemoryBlobData
 * @see FileBlobData
 * @see OutputBlobDefinition
 * @see TaskDefinition
 */
public final class InputBlobDefinition implements BlobDefinition {

  private String name;
  private boolean manualDeletion;
  private final BlobData blobData;

  /**
   * Creates an input blob definition wrapping the specified blob data.
   * <p>
   * This constructor initializes the blob with:
   * <ul>
   *   <li>Empty name (can be set via {@link #withName(String)})</li>
   *   <li>Automatic deletion (can be changed via {@link #withManualDeletion(boolean)})</li>
   * </ul>
   * <p>
   * Use factory methods like {@link #from(byte[])} or {@link #from(File)} for more
   * convenient creation.
   *
   * @param blobData the data source for this blob
   */
  private InputBlobDefinition(BlobData blobData) {
    this.blobData = blobData;
    this.name = "";
    this.manualDeletion = false;
  }

  @Override
  public String name() {
    return name;
  }

  @Override
  public boolean manualDeletion() {
    return manualDeletion;
  }

  /**
   * Returns the data source for this input blob.
   * <p>
   * The returned {@link BlobData} provides access to the actual data content.
   *
   * @return the blob data source, never null
   */
  public BlobData data() {
    return blobData;
  }

  /**
   * Sets the name of this input blob.
   * <p>
   * The name serves as a logical identifier.
   * If the provided name is {@code null}, it is converted to an empty string.
   * <p>
   * This method modifies the current instance and returns it to enable method chaining.
   *
   * @param name the user-defined name for this blob, or null for empty string
   * @return this instance for method chaining
   */
  public InputBlobDefinition withName(String name) {
    this.name = requireNonNullElse(name, "");
    return this;
  }

  /**
   * Sets the manual deletion policy for this input blob.
   * <p>
   * When set to {@code true}, the user is responsible for explicitly deleting the
   * blob data from the underlying object storage. When {@code false} (default),
   * ArmoniK automatically manages the blob lifecycle.
   * <p>
   * This method modifies the current instance and returns it to enable method chaining.
   *
   * @param manualDeletion whether the user is responsible for deleting the data
   * @return this instance for method chaining
   * @see #withManualDeletion()
   */
  public InputBlobDefinition withManualDeletion(boolean manualDeletion) {
    this.manualDeletion = manualDeletion;
    return this;
  }

  /**
   * Enables manual deletion for this input blob.
   * <p>
   * This is a convenience method equivalent to {@code withManualDeletion(true)}.
   * When manual deletion is enabled, the user must explicitly delete the blob data
   * from the underlying object storage.
   * <p>
   * This method modifies the current instance and returns it to enable method chaining.
   *
   * @return this instance for method chaining
   * @see #withManualDeletion(boolean)
   */
  public InputBlobDefinition withManualDeletion() {
    return this.withManualDeletion(true);
  }

  /**
   * Creates an input blob definition from the specified blob data source.
   * <p>
   * This factory method wraps any {@link BlobData} implementation. The created blob
   * has default properties that can be customized using fluent methods:
   * <ul>
   *   <li>Empty name (use {@link #withName(String)} to set)</li>
   *   <li>Automatic deletion (use {@link #withManualDeletion()} to change)</li>
   * </ul>
   *
   * @param blobData the data source for the blob
   * @return a new input blob definition
   * @throws NullPointerException if blobData is null
   * @see InMemoryBlobData
   * @see FileBlobData
   */
  public static InputBlobDefinition from(BlobData blobData) {
    requireNonNull(blobData, "blobData must not be null");
    return new InputBlobDefinition(blobData);
  }

  /**
   * Creates an input blob definition from an in-memory byte array.
   * <p>
   * This convenience factory method wraps the byte array in an {@link InMemoryBlobData}
   * and creates an input blob definition. The created blob has default properties:
   * <ul>
   *   <li>Empty name (use {@link #withName(String)} to set)</li>
   *   <li>Automatic deletion (use {@link #withManualDeletion()} to change)</li>
   * </ul>
   * <p>
   * <strong>Note:</strong> The byte array is used directly without defensive copying.
   * Do not modify the array after passing it to this method.
   *
   * @param data the byte array containing the blob data
   * @return a new input blob definition backed by in-memory data
   * @throws NullPointerException if data is null
   * @see InMemoryBlobData
   */
  public static InputBlobDefinition from(byte[] data) {
    requireNonNull(data, "data must not be null");
    return new InputBlobDefinition(InMemoryBlobData.from(data));
  }

  /**
   * Creates an input blob definition from a file.
   * <p>
   * This convenience factory method wraps the file in a {@link FileBlobData} and creates
   * an input blob definition. The created blob has default properties:
   * <ul>
   *   <li>Empty name (use {@link #withName(String)} to set)</li>
   *   <li>Automatic deletion (use {@link #withManualDeletion()} to change)</li>
   * </ul>
   * <p>
   * The file must exist and be readable. Data is streamed from the file during upload,
   * making this memory-efficient for large files.
   *
   * @param file the file containing the blob data
   * @return a new input blob definition backed by file data
   * @throws NullPointerException if file is null
   * @throws ArmoniKException     if the file does not exist or cannot be read
   * @see FileBlobData
   */
  public static InputBlobDefinition from(File file) {
    requireNonNull(file, "file must not be null");
    return new InputBlobDefinition(FileBlobData.from(file));
  }
}
