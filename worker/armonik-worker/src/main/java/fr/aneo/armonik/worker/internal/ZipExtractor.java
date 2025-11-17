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
package fr.aneo.armonik.worker.internal;

import fr.aneo.armonik.worker.domain.ArmoniKException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import static java.nio.file.StandardOpenOption.*;

/**
 * Safely extracts ZIP files with security checks to prevent malicious content.
 * <p>
 * This extractor implements multiple security validations:
 * </p>
 * <ul>
 *   <li><strong>Path traversal prevention:</strong> Rejects entries attempting to escape target directory</li>
 *   <li><strong>ZIP bomb protection:</strong> Limits maximum extraction size to prevent memory exhaustion</li>
 *   <li><strong>Entry validation:</strong> Ensures all entries resolve within extraction directory</li>
 * </ul>
 *
 * <h2>Security Limits</h2>
 * <ul>
 *   <li><strong>Default max extraction size:</strong> 750 MB (prevents ZIP bombs)</li>
 *   <li><strong>Configurable via:</strong> {@code ARMONIK_WORKER_MAX_ZIP_EXTRACTION_SIZE_MB} environment variable</li>
 *   <li><strong>Buffer size:</strong> 8 KB (balances performance and memory)</li>
 * </ul>
 *
 * <h2>Configuration</h2>
 * <p>
 * Set {@code ARMONIK_WORKER_MAX_ZIP_EXTRACTION_SIZE_MB} to override the default 750 MB limit:
 * </p>
 * <pre>
 * export ARMONIK_WORKER_MAX_ZIP_EXTRACTION_SIZE_MB=1000  # Allow up to 1 GB
 * </pre>
 *
 * <h2>Thread Safety</h2>
 * <p>
 * This class is stateless and thread-safe.
 * </p>
 */
final class ZipExtractor {
  private static final Logger logger = LoggerFactory.getLogger(ZipExtractor.class);

  private static final String ENV_MAX_EXTRACTION_SIZE_MB = "ARMONIK_WORKER_MAX_ZIP_EXTRACTION_SIZE_MB";
  private static final long DEFAULT_MAX_EXTRACTION_SIZE_MB = 750L;  // 750 MB
  private static final int BUFFER_SIZE = 8192; // 8 KB

  private static long maxExtractionSize;

  static {
    loadMaxExtractionSize();
  }

  private ZipExtractor() {
  }

  /**
   * Loads the maximum extraction size from environment variable.
   * <p>
   * This method is called from the static initializer and can be called again
   * from tests to reload the configuration when environment variables change.
   * </p>
   */
  static void loadMaxExtractionSize() {
    var envValue = System.getenv(ENV_MAX_EXTRACTION_SIZE_MB);

    if (envValue == null || envValue.trim().isEmpty()) {
      logger.debug("Using default max extraction size: {} MB", DEFAULT_MAX_EXTRACTION_SIZE_MB);
      maxExtractionSize = DEFAULT_MAX_EXTRACTION_SIZE_MB * 1024 * 1024;
    } else {
      try {
        long sizeMB = Long.parseLong(envValue.trim());
        if (sizeMB <= 0) {
          logger.warn("Invalid max extraction size in {}: {} (must be positive). Using default: {} MB", ENV_MAX_EXTRACTION_SIZE_MB, envValue, DEFAULT_MAX_EXTRACTION_SIZE_MB);
          maxExtractionSize = DEFAULT_MAX_EXTRACTION_SIZE_MB * 1024 * 1024;

        } else {
          logger.info("Using configured max extraction size: {} MB", sizeMB);
          maxExtractionSize = sizeMB * 1024 * 1024;
        }
      } catch (NumberFormatException e) {
        logger.warn("Invalid number format for {}: {}. Using default: {} MB", ENV_MAX_EXTRACTION_SIZE_MB, envValue, DEFAULT_MAX_EXTRACTION_SIZE_MB);
        maxExtractionSize = DEFAULT_MAX_EXTRACTION_SIZE_MB * 1024 * 1024;
      }
    }
  }

  /**
   * Extracts a ZIP file to the specified destination directory.
   * <p>
   * The maximum extraction size is determined at class loading time by:
   * </p>
   * <ol>
   *   <li>{@code ARMONIK_WORKER_MAX_ZIP_EXTRACTION_SIZE_MB} environment variable (in MB)</li>
   *   <li>Default of 750 MB if environment variable is not set or invalid</li>
   * </ol>
   * <p>
   * This method:
   * </p>
   * <ol>
   *   <li>Creates the destination directory if it doesn't exist</li>
   *   <li>Validates each ZIP entry for security violations</li>
   *   <li>Extracts files and directories while preserving structure</li>
   *   <li>Tracks total extracted size to prevent ZIP bombs</li>
   * </ol>
   *
   * @param zipFile     the ZIP file to extract; must exist and be readable
   * @param destination the destination directory; created if doesn't exist
   * @return the destination directory path (same as input)
   * @throws ArmoniKException if extraction fails, security violation detected, or ZIP bomb detected
   */
  static Path extract(Path zipFile, Path destination) {
    logger.debug("Extracting ZIP: {} to {} (max size: {} MB)", zipFile, destination, maxExtractionSize / 1024 / 1024);

    try {
      Files.createDirectories(destination);
    } catch (IOException e) {
      throw new ArmoniKException("Failed to create extraction directory: " + destination, e);
    }

    long totalExtracted = 0;
    int fileCount = 0;

    try (var fis = Files.newInputStream(zipFile);
         var zis = new ZipInputStream(fis)) {

      ZipEntry entry;
      while ((entry = zis.getNextEntry()) != null) {
        var targetPath = validateAndResolveEntry(destination, entry);

        if (entry.isDirectory()) {
          Files.createDirectories(targetPath);
        } else {
          Files.createDirectories(targetPath.getParent());
          long extracted = extractFile(zis, targetPath);
          totalExtracted += extracted;
          fileCount++;

          if (totalExtracted > maxExtractionSize) {
            throw new ArmoniKException("ZIP bomb detected: extraction size exceeds " + (maxExtractionSize / 1024 / 1024) + " MB limit");
          }
        }

        zis.closeEntry();
      }

      logger.info("ZIP extracted successfully: {} files, {} MB", fileCount, totalExtracted / 1024 / 1024);

      return destination;

    } catch (IOException e) {
      throw new ArmoniKException("Failed to extract ZIP file: " + zipFile, e);
    }
  }

  /**
   * Validates a ZIP entry and resolves its target path.
   */
  private static Path validateAndResolveEntry(Path destination, ZipEntry entry) {
    var entryName = entry.getName();

    if (entryName.startsWith("/") || entryName.startsWith("\\")) {
      throw new ArmoniKException("Security violation: ZIP entry has absolute path: " + entryName);
    }

    try {
      return PathValidator.resolveDescendant(destination, entryName);
    } catch (ArmoniKException e) {
      throw new ArmoniKException("Security violation: ZIP entry attempts directory traversal: " + entryName, e);
    }
  }

  /**
   * Extracts a single file from the ZIP input stream.
   *
   * @return number of bytes written
   */
  private static long extractFile(InputStream input, Path target) throws IOException {
    long totalWritten = 0;
    byte[] buffer = new byte[BUFFER_SIZE];

    try (var output = Files.newOutputStream(target, CREATE, TRUNCATE_EXISTING, WRITE)) {
      int bytesRead;
      while ((bytesRead = input.read(buffer)) != -1) {
        output.write(buffer, 0, bytesRead);
        totalWritten += bytesRead;
      }
    }

    return totalWritten;
  }
}
