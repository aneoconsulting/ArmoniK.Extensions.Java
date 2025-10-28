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

import fr.aneo.armonik.client.exception.ArmoniKException;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

import static java.util.Objects.requireNonNull;

/**
 * Represents data content from a file.
 * <p>
 * {@code FileBlobData} encapsulates data stored in a file on the local filesystem.
 * This is memory efficient for large files as the data is streamed incrementally
 * without loading the entire content into memory.
 * <p>
 * <strong>Memory Efficiency:</strong>
 * <ul>
 *   <li>Only upload buffer in memory</li>
 *   <li>Suitable for files of any size</li>
 *   <li>Recommended for files larger than a few megabytes</li>
 * </ul>
 * <p>
 * <strong>File Access:</strong> Each call to {@link #stream()} opens a new
 * {@link FileInputStream} to the file. The file must exist and be readable
 * when the stream is opened.
 * <p>
 * This class is immutable and thread-safe.
 *
 * @see BlobData
 * @see InMemoryBlobData
 */
public final class FileBlobData extends BlobData {
  private final File file;

  /**
   * Creates file blob data from the specified file.
   *
   * @param file the file containing the data
   * @throws NullPointerException if file is null
   * @throws IOException          if the file does not exist or cannot be read
   */
  private FileBlobData(File file) throws IOException {
    requireNonNull(file, "file must not be null");

    if (!file.exists()) {
      throw new IOException("File not found: " + file.getAbsolutePath());
    }
    if (!file.canRead()) {
      throw new IOException("File not readable: " + file.getAbsolutePath());
    }
    this.file = file;
  }

  /**
   * Returns the file containing the data.
   *
   * @return the file, never null
   */
  public File file() {
    return file;
  }

  @Override
  public InputStream stream() throws IOException {
    return new FileInputStream(file);
  }

  /**
   * Creates file blob data from the specified file.
   *
   * @param file the file containing the data
   * @return a new file blob data instance
   * @throws NullPointerException if file is null
   * @throws ArmoniKException     if the file does not exist or cannot be read
   */
  public static FileBlobData from(File file) {
    try {
      return new FileBlobData(file);
    } catch (IOException e) {
      throw new ArmoniKException("Failed to read FileBlobData from file: " + file, e);
    }
  }

  /**
   * Creates file blob data from the specified file path.
   *
   * @param filePath the path to the file containing the data
   * @return a new file blob data instance
   * @throws NullPointerException if filePath is null
   * @throws ArmoniKException     if the file does not exist or cannot be read
   */
  public static FileBlobData from(String filePath) {
    requireNonNull(filePath, "filePath must not be null");
    try {
      return new FileBlobData(new File(filePath));
    } catch (IOException e) {
      throw new ArmoniKException("Failed to read FileBlobData from filePath: " + filePath, e);
    }
  }
}
