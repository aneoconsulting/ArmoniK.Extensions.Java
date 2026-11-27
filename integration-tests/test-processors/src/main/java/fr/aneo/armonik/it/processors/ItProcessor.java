/*
 * Copyright (C) 2025 ANEO (armonik@aneo.fr)
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
package fr.aneo.armonik.it.processors;

import fr.aneo.armonik.worker.domain.TaskContext;
import fr.aneo.armonik.worker.domain.TaskOutcome;
import fr.aneo.armonik.worker.domain.TaskProcessor;
import fr.aneo.armonik.worker.domain.definition.blob.InputBlobDefinition;
import fr.aneo.armonik.worker.domain.definition.task.TaskDefinition;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Task processor of the integration tests.
 * <p>
 * Without a {@code cmd} input it copies input {@code A} to output {@code result}, like the README's
 * example. A {@code cmd} input holds commands separated by {@code ;}, run in order:
 * <ul>
 *   <li>{@code copy}: write {@code A} to {@code result} (the default when no command writes it)</li>
 *   <li>{@code append:X}: write {@code A} followed by {@code X} to {@code result}</li>
 *   <li>{@code sleep:MS}: sleep {@code MS} milliseconds</li>
 *   <li>{@code fail}: return an error outcome</li>
 *   <li>{@code throw}: throw an exception</li>
 *   <li>{@code failonce:KEY}: return an error the first time this worker sees {@code KEY}</li>
 *   <li>{@code crashonce:KEY}: halt the worker JVM the first time this worker container sees {@code KEY}
 *       (a marker file in the temporary directory survives the container restart)</li>
 *   <li>{@code big:N}: write {@code N} bytes of a fixed pattern to {@code result}</li>
 *   <li>{@code sub:X}: submit a subtask that writes {@code A} followed by {@code X} to this task's {@code result}</li>
 * </ul>
 */
public class ItProcessor implements TaskProcessor {
  private static final Set<String> SEEN = ConcurrentHashMap.newKeySet();

  @Override
  public TaskOutcome processTask(TaskContext context) {
    var commands = context.hasInput("cmd") ? context.getInput("cmd").asString(StandardCharsets.UTF_8) : "";
    var written = false;
    try {
      for (var command : commands.split(";")) {
        if (command.isBlank()) continue;
        var colon = command.indexOf(':');
        var name = colon < 0 ? command : command.substring(0, colon);
        var argument = colon < 0 ? "" : command.substring(colon + 1);
        switch (name) {
          case "copy" -> {
            copy(context, new byte[0]);
            written = true;
          }
          case "append" -> {
            copy(context, argument.getBytes(StandardCharsets.UTF_8));
            written = true;
          }
          case "sleep" -> Thread.sleep(Long.parseLong(argument));
          case "fail" -> {
            return TaskOutcome.error("requested failure");
          }
          case "throw" -> throw new IllegalStateException("requested exception");
          case "failonce" -> {
            if (SEEN.add(argument)) return TaskOutcome.error("first attempt of " + argument + " fails");
          }
          case "crashonce" -> {
            var marker = crashMarkerDirectory().resolve("it-crashonce-" + argument);
            if (!java.nio.file.Files.exists(marker)) {
              java.nio.file.Files.createFile(marker);
              Runtime.getRuntime().halt(1);
            }
          }
          case "big" -> {
            context.getOutput("result").write(new PatternStream(Long.parseLong(argument)));
            written = true;
          }
          case "sub" -> {
            var child = new TaskDefinition()
              .withInput("A", context.getInput("A"))
              .withInput("cmd", InputBlobDefinition.from(("append:" + argument).getBytes(StandardCharsets.UTF_8)))
              .withOutput("result", context.getOutput("result"))
              .withWorkerLibrary(context.workerLibrary());
            context.submitTask(child);
            written = true;
          }
          default -> {
            return TaskOutcome.error("unknown command " + command);
          }
        }
      }
      if (!written) copy(context, new byte[0]);
      return TaskOutcome.SUCCESS;
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return TaskOutcome.error(e);
    } catch (IOException e) {
      return TaskOutcome.error(e);
    }
  }

  private static void copy(TaskContext context, byte[] suffix) throws IOException {
    var input = context.getInput("A");
    if (suffix.length == 0) {
      try (var stream = input.stream()) {
        context.getOutput("result").write(stream);
      }
      return;
    }
    var buffer = new ByteArrayOutputStream();
    try (var stream = input.stream()) {
      stream.transferTo(buffer);
    }
    buffer.write(suffix);
    context.getOutput("result").write(new ByteArrayInputStream(buffer.toByteArray()));
  }

  /** {@code size} bytes where byte {@code i} is {@code (i * 31 + 7) & 0xff}, generated on the fly. */
  static final class PatternStream extends InputStream {
    private final long size;
    private long position;

    PatternStream(long size) {
      this.size = size;
    }

    @Override
    public int read() {
      if (position >= size) return -1;
      return (int) ((position++ * 31 + 7) & 0xff);
    }

    @Override
    public int read(byte[] buffer, int offset, int length) {
      if (position >= size) return -1;
      var count = (int) Math.min(length, size - position);
      for (int i = 0; i < count; i++) buffer[offset + i] = (byte) ((position++ * 31 + 7) & 0xff);
      return count;
    }
  }

  /**
   * Where crashonce leaves its marker: it must outlive the worker container, which the crash restarts.
   * On Kubernetes {@code /cache} is an emptyDir shared by the pod, while the container's own {@code /tmp}
   * is reset; with docker the container's file system, hence {@code /tmp}, survives a restart.
   */
  private static java.nio.file.Path crashMarkerDirectory() {
    var cache = java.nio.file.Paths.get("/cache");
    return java.nio.file.Files.isDirectory(cache) && java.nio.file.Files.isWritable(cache)
      ? cache
      : java.nio.file.Paths.get(System.getProperty("java.io.tmpdir"));
  }
}
