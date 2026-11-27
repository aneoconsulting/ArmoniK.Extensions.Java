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
package fr.aneo.armonik.client.it;

import fr.aneo.armonik.client.ArmoniKClient;
import fr.aneo.armonik.client.ArmoniKConfig;
import fr.aneo.armonik.client.BlobCompletionListener;
import fr.aneo.armonik.client.BlobHandle;
import fr.aneo.armonik.client.BlobId;
import fr.aneo.armonik.client.ChannelPool;
import fr.aneo.armonik.client.SessionHandle;
import fr.aneo.armonik.client.SessionId;
import fr.aneo.armonik.client.TaskConfiguration;
import fr.aneo.armonik.client.TaskHandle;
import fr.aneo.armonik.client.WorkerLibrary;
import fr.aneo.armonik.client.definition.SessionDefinition;
import fr.aneo.armonik.client.definition.TaskDefinition;
import fr.aneo.armonik.client.definition.blob.InputBlobDefinition;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.jar.JarOutputStream;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

/**
 * Shared helpers of the integration tests, which run against a real ArmoniK cluster.
 * <p>
 * The tests are skipped unless an endpoint is given, so that a build without a cluster stays green:
 * <pre>
 * mvn -f worker install -DskipTests            # armonik-worker-domain, for the test processors
 * mvn -f armonik-client install -DskipTests    # the client under test
 * mvn -f integration-tests verify -Darmonik.it.endpoint=http://localhost:5001 -Darmonik.it.partition=TestPartition1
 * </pre>
 * Settings, as system properties or environment variables:
 * <ul>
 *   <li>{@code armonik.it.endpoint} / {@code ARMONIK_IT_ENDPOINT}: control plane endpoint; tests skipped when empty</li>
 *   <li>{@code armonik.it.partition} / {@code ARMONIK_IT_PARTITION}: a partition served by the dynamic Java worker</li>
 *   <li>{@code armonik.it.stress} / {@code ARMONIK_IT_STRESS}: {@code true} runs the long and heavy tests too</li>
 *   <li>{@code armonik.it.restart} / {@code ARMONIK_IT_RESTART}: name of the control plane docker container the
 *       restart test may restart, e.g. {@code armonik.control.submitter}; that test is skipped when empty</li>
 *   <li>{@code armonik.it.worker-crash} / {@code ARMONIK_IT_WORKER_CRASH}: {@code true} runs the test that crashes
 *       the worker process</li>
 *   <li>For an {@code https://} endpoint: {@code armonik.it.ca-cert} / {@code ARMONIK_IT_CA_CERT}, a PEM file of
 *       the CA that signed the server certificate; {@code armonik.it.insecure} / {@code ARMONIK_IT_INSECURE},
 *       {@code true} to skip the validation of the server certificate; for mutual TLS,
 *       {@code armonik.it.client-p12} / {@code ARMONIK_IT_CLIENT_P12} and {@code armonik.it.client-p12-password} /
 *       {@code ARMONIK_IT_CLIENT_P12_PASSWORD}, or {@code armonik.it.client-cert} / {@code ARMONIK_IT_CLIENT_CERT}
 *       and {@code armonik.it.client-key} / {@code ARMONIK_IT_CLIENT_KEY} in PEM</li>
 * </ul>
 * <p>
 * Without {@code armonik.it.stress}, the suite runs in a few minutes. Tests that reproduce an open bug
 * are {@code @Disabled} with a link to its issue.
 * <p>
 * The worker of the partition must be the dynamic Java worker: the tests upload
 * {@code fr.aneo.armonik.it.processors.ItProcessor} as a {@link WorkerLibrary} in every session.
 */
public final class ItSupport {
  public static final String ENDPOINT = setting("armonik.it.endpoint", "ARMONIK_IT_ENDPOINT", "");
  public static final String PARTITION = setting("armonik.it.partition", "ARMONIK_IT_PARTITION", "TestPartition1");
  public static final boolean STRESS = Boolean.parseBoolean(setting("armonik.it.stress", "ARMONIK_IT_STRESS", "false"));
  public static final String RESTART_CONTAINER = setting("armonik.it.restart", "ARMONIK_IT_RESTART", "");
  public static final boolean WORKER_CRASH = Boolean.parseBoolean(setting("armonik.it.worker-crash", "ARMONIK_IT_WORKER_CRASH", "false"));
  static final String CA_CERT = setting("armonik.it.ca-cert", "ARMONIK_IT_CA_CERT", "");
  static final boolean INSECURE = Boolean.parseBoolean(setting("armonik.it.insecure", "ARMONIK_IT_INSECURE", "false"));
  static final String CLIENT_P12 = setting("armonik.it.client-p12", "ARMONIK_IT_CLIENT_P12", "");
  static final String CLIENT_P12_PASSWORD = setting("armonik.it.client-p12-password", "ARMONIK_IT_CLIENT_P12_PASSWORD", "");
  static final String CLIENT_CERT = setting("armonik.it.client-cert", "ARMONIK_IT_CLIENT_CERT", "");
  static final String CLIENT_KEY = setting("armonik.it.client-key", "ARMONIK_IT_CLIENT_KEY", "");
  static final String LIBRARY_PATH = "it-processors.jar";
  static final String SYMBOL = "fr.aneo.armonik.it.processors.ItProcessor";

  private static final Set<SessionId> CREATED_SESSIONS = ConcurrentHashMap.newKeySet();
  private static final Map<SessionId, WorkerLibrary> LIBRARIES = new ConcurrentHashMap<>();
  private static volatile byte[] libraryZip;

  private ItSupport() {
  }

  /** Whether an endpoint is configured; used by {@code @EnabledIf}. */
  public static boolean enabled() {
    return !ENDPOINT.isBlank();
  }

  /** Whether the long and heavy tests run too. */
  public static boolean stressEnabled() {
    return enabled() && STRESS;
  }

  /**
   * Whether the test that crashes the worker runs. A crashed worker is restarted by its container
   * runtime; outside Kubernetes the restarted worker may not reach its agent again, which fails the
   * tests that follow, so it runs only when asked.
   */
  public static boolean workerCrashEnabled() {
    return enabled() && WORKER_CRASH;
  }

  /** Whether the control plane restart test runs. */
  public static boolean restartEnabled() {
    return stressEnabled() && !RESTART_CONTAINER.isBlank();
  }

  private static String setting(String property, String variable, String fallback) {
    var value = System.getProperty(property);
    if (value == null || value.isBlank()) value = System.getenv(variable);
    return value == null || value.isBlank() ? fallback : value.trim();
  }

  // ---------------------------------------------------------------- clients and sessions

  public static ArmoniKClient newClient() {
    return newClient(ENDPOINT, builder -> {
    });
  }

  public static ArmoniKClient newClient(Consumer<ArmoniKConfig.Builder> customizer) {
    return newClient(ENDPOINT, customizer);
  }

  /**
   * Creates a client for {@code endpoint}: plaintext for {@code http://}, TLS with the configured CA,
   * validation and client certificate for {@code https://}.
   */
  public static ArmoniKClient newClient(String endpoint, Consumer<ArmoniKConfig.Builder> customizer) {
    var builder = ArmoniKConfig.builder().endpoint(endpoint);
    if (endpoint.startsWith("https://")) {
      if (!CA_CERT.isBlank()) builder.withCaCert(CA_CERT);
      if (INSECURE) builder.withoutSslValidation();
      if (!CLIENT_P12.isBlank()) {
        builder.withClientP12(CLIENT_P12, CLIENT_P12_PASSWORD.isBlank() ? null : CLIENT_P12_PASSWORD);
      } else if (!CLIENT_CERT.isBlank()) {
        builder.withClientCert(CLIENT_CERT, CLIENT_KEY);
      }
    }
    customizer.accept(builder);
    return new ArmoniKClient(builder.build());
  }

  public static SessionHandle openSession(ArmoniKClient client, BlobCompletionListener listener) {
    return openSession(client, listener, TaskConfiguration.defaultConfigurationWithPartition(PARTITION));
  }

  public static SessionHandle openSession(ArmoniKClient client, BlobCompletionListener listener, TaskConfiguration configuration) {
    var session = client.openSession(new SessionDefinition(Set.of(PARTITION), configuration, listener));
    CREATED_SESSIONS.add(session.sessionInfo().id());
    return session;
  }

  /** Remembers a session created another way, so that {@link #cancelCreatedSessions()} cancels it. */
  public static void track(SessionId sessionId) {
    CREATED_SESSIONS.add(sessionId);
  }

  /**
   * Cancels every session the tests created, with a fresh client: paused sessions would otherwise keep
   * their tasks queued on the shared cluster. Errors are ignored, sessions may be gone already.
   */
  public static void cancelCreatedSessions() {
    if (!enabled() || CREATED_SESSIONS.isEmpty()) return;
    var ids = new ArrayList<>(CREATED_SESSIONS);
    CREATED_SESSIONS.clear();
    LIBRARIES.clear();
    try (var admin = newClient()) {
      var cancels = ids.stream()
                       .map(id -> admin.cancelSession(id).toCompletableFuture().handle((state, error) -> null))
                       .toArray(CompletableFuture[]::new);
      CompletableFuture.allOf(cancels).get(2, TimeUnit.MINUTES);
    } catch (Exception e) {
      System.out.println("Could not cancel every test session: " + e);
    }
  }

  // ---------------------------------------------------------------- tasks

  /** The test processors of this session, uploaded once per session. */
  public static WorkerLibrary library(SessionHandle session) {
    return LIBRARIES.computeIfAbsent(session.sessionInfo().id(), id ->
      new WorkerLibrary(LIBRARY_PATH, SYMBOL, session.createBlob(InputBlobDefinition.from(libraryZip()).withName("it-processors.zip"))));
  }

  public static TaskDefinition task(SessionHandle session, byte[] a) {
    return task(session, a, null);
  }

  public static TaskDefinition task(SessionHandle session, byte[] a, String cmd) {
    var definition = new TaskDefinition().withInput("A", InputBlobDefinition.from(a))
                                         .withOutput("result")
                                         .withWorkerLibrary(library(session));
    if (cmd != null) definition.withInput("cmd", InputBlobDefinition.from(cmd.getBytes(StandardCharsets.UTF_8)));
    return definition;
  }

  public static TaskDefinition task(SessionHandle session, BlobHandle a, String cmd) {
    var definition = new TaskDefinition().withInput("A", a)
                                         .withOutput("result")
                                         .withWorkerLibrary(library(session));
    if (cmd != null) definition.withInput("cmd", InputBlobDefinition.from(cmd.getBytes(StandardCharsets.UTF_8)));
    return definition;
  }

  public static List<TaskDefinition> copyTasks(SessionHandle session, int count, String prefix) {
    var library = library(session);
    var tasks = new ArrayList<TaskDefinition>(count);
    for (int i = 0; i < count; i++) {
      tasks.add(new TaskDefinition().withInput("A", InputBlobDefinition.from((prefix + i).getBytes(StandardCharsets.UTF_8)))
                                    .withOutput("result")
                                    .withWorkerLibrary(library));
    }
    return tasks;
  }

  public static BlobHandle output(TaskHandle handle) {
    return handle.outputs().get("result");
  }

  public static BlobId outputId(TaskHandle handle, Duration timeout) {
    return within(timeout, "id of the output", output(handle).deferredBlobInfo()).id();
  }

  public static CompletableFuture<Void> allTaskInfos(List<TaskHandle> handles) {
    return CompletableFuture.allOf(handles.stream()
                                          .map(TaskHandle::deferredTaskInfo)
                                          .map(CompletionStage::toCompletableFuture)
                                          .toArray(CompletableFuture[]::new));
  }

  public static byte[] utf8(String value) {
    return value.getBytes(StandardCharsets.UTF_8);
  }

  public static String utf8(byte[] value) {
    return new String(value, StandardCharsets.UTF_8);
  }

  /** The bytes {@code big:N} of the test processor writes. */
  public static byte[] pattern(int size) {
    var data = new byte[size];
    for (int i = 0; i < size; i++) data[i] = (byte) (((long) i * 31 + 7) & 0xff);
    return data;
  }

  /** A zip holding a jar of the compiled test processors, built once from their class directory. */
  static byte[] libraryZip() {
    if (libraryZip == null) {
      synchronized (ItSupport.class) {
        if (libraryZip == null) libraryZip = buildLibraryZip();
      }
    }
    return libraryZip;
  }

  private static byte[] buildLibraryZip() {
    var classes = Paths.get(setting("armonik.it.processors", "ARMONIK_IT_PROCESSORS", "../test-processors/target/classes"));
    try {
      var jar = new ByteArrayOutputStream();
      try (var out = new JarOutputStream(jar); Stream<Path> files = Files.walk(classes)) {
        for (var file : files.filter(Files::isRegularFile).sorted().toList()) {
          out.putNextEntry(new ZipEntry(classes.relativize(file).toString().replace('\\', '/')));
          out.write(Files.readAllBytes(file));
          out.closeEntry();
        }
      }
      var zip = new ByteArrayOutputStream();
      try (var out = new ZipOutputStream(zip)) {
        out.putNextEntry(new ZipEntry(LIBRARY_PATH));
        out.write(jar.toByteArray());
        out.closeEntry();
      }
      return zip.toByteArray();
    } catch (IOException e) {
      throw new UncheckedIOException("Could not pack the test processors from " + classes.toAbsolutePath(), e);
    }
  }

  // ---------------------------------------------------------------- waiting

  /** Waits for {@code stage}; a timeout fails the test with the threads blocked in the SDK. */
  public static <T> T within(Duration timeout, String what, CompletionStage<T> stage) {
    try {
      return stage.toCompletableFuture().get(timeout.toMillis(), TimeUnit.MILLISECONDS);
    } catch (TimeoutException e) {
      throw new AssertionError(what + ": not done after " + timeout.toSeconds() + " s. " + sdkThreads(), e);
    } catch (ExecutionException e) {
      throw new AssertionError(what + " failed: " + e.getCause(), e.getCause());
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new AssertionError(what + ": interrupted", e);
    }
  }

  /** Runs a blocking call on its own daemon thread, so that a hang fails the test instead of blocking it. */
  public static <T> T call(Duration timeout, String what, Callable<T> callable) {
    var result = new CompletableFuture<T>();
    var thread = new Thread(() -> {
      try {
        result.complete(callable.call());
      } catch (Throwable throwable) {
        result.completeExceptionally(throwable);
      }
    }, "it-call-" + what.replaceAll("\\W+", "-"));
    thread.setDaemon(true);
    thread.start();
    return within(timeout, what, result);
  }

  public static void run(Duration timeout, String what, Runnable runnable) {
    call(timeout, what, () -> {
      runnable.run();
      return null;
    });
  }

  public static boolean await(Duration timeout, BooleanSupplier condition) {
    var deadline = System.nanoTime() + timeout.toNanos();
    while (!condition.getAsBoolean()) {
      if (System.nanoTime() - deadline > 0) return false;
      sleep(20);
    }
    return true;
  }

  public static void sleep(long millis) {
    try {
      Thread.sleep(millis);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new AssertionError("interrupted", e);
    }
  }

  public static long millisSince(long startNanos) {
    return TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
  }

  // ---------------------------------------------------------------- introspection

  /** Threads whose stack is inside the SDK's channel pool or waiting for outputs, with their top frames. */
  public static String sdkThreads() {
    var lines = new ArrayList<String>();
    for (var entry : Thread.getAllStackTraces().entrySet()) {
      var stack = entry.getValue();
      var inSdk = Arrays.stream(stack).anyMatch(frame -> frame.getClassName().startsWith("fr.aneo.armonik.client.")
        && !frame.getClassName().startsWith("fr.aneo.armonik.client.it."));
      if (!inSdk || entry.getKey().getState() == Thread.State.RUNNABLE) continue;
      var top = Arrays.stream(stack).limit(12).map(StackTraceElement::toString).collect(Collectors.joining(" < "));
      lines.add(entry.getKey().getName() + " " + entry.getKey().getState() + ": " + top);
    }
    return lines.isEmpty() ? "No thread blocked in the SDK." : lines.size() + " threads blocked in the SDK:\n  " + String.join("\n  ", lines);
  }

  /** Number of threads currently blocked in a synchronous acquisition of the channel pool. */
  public static int threadsBlockedInPool() {
    var count = 0;
    for (var entry : Thread.getAllStackTraces().entrySet()) {
      if (entry.getKey().getState() == Thread.State.RUNNABLE) continue;
      for (var frame : entry.getValue()) {
        if (frame.getClassName().endsWith("ManagedChannelPool") && (frame.getMethodName().equals("execute") || frame.getMethodName().equals("acquire"))) {
          count++;
          break;
        }
      }
    }
    return count;
  }

  /** {@code {channels, available}} of the client's pool, read by reflection: in use = channels - available. */
  public static int[] poolState(ArmoniKClient client, String field) {
    try {
      var declared = ArmoniKClient.class.getDeclaredField(field);
      declared.setAccessible(true);
      var pool = (ChannelPool) declared.get(client);
      return new int[]{pool.size(), pool.availableChannels()};
    } catch (ReflectiveOperationException e) {
      throw new IllegalStateException("No pool field " + field + " in ArmoniKClient", e);
    }
  }

  public static int channelsInUse(ArmoniKClient client, String field) {
    var state = poolState(client, field);
    return state[0] - state[1];
  }

  /** Number of TCP connections of this JVM to {@code port}, from /proc; -1 when unavailable. */
  public static int connectionsTo(int port) {
    try {
      var inodes = new HashSet<String>();
      try (var fds = Files.list(Paths.get("/proc/self/fd"))) {
        for (var fd : fds.toList()) {
          try {
            var target = Files.readSymbolicLink(fd).toString();
            if (target.startsWith("socket:[")) inodes.add(target.substring(8, target.length() - 1));
          } catch (IOException ignored) {
            // closed meanwhile
          }
        }
      }
      var portHex = String.format(":%04X", port);
      var count = 0;
      for (var table : List.of("/proc/net/tcp", "/proc/net/tcp6")) {
        var path = Paths.get(table);
        if (!Files.exists(path)) continue;
        for (var line : Files.readAllLines(path)) {
          var columns = line.trim().split("\\s+");
          if (columns.length < 10 || !columns[2].endsWith(portHex)) continue;
          if (!columns[3].equals("01")) continue; // ESTABLISHED
          if (inodes.contains(columns[9])) count++;
        }
      }
      return count;
    } catch (IOException e) {
      return -1;
    }
  }

  public static int endpointPort() {
    return java.net.URI.create(ENDPOINT).getPort();
  }

  public static List<Thread> liveThreads() {
    return new ArrayList<>(Thread.getAllStackTraces().keySet());
  }

  public static long nonDaemonThreads() {
    return liveThreads().stream().filter(thread -> !thread.isDaemon() && thread.isAlive()).count();
  }

  // ---------------------------------------------------------------- metrics

  /** Appends a measurement to the run's metrics file and prints it. */
  public static void metric(String test, String key, Object value) {
    var label = setting("armonik.it.label", "ARMONIK_IT_LABEL", "local");
    var line = label + "\t" + test + "\t" + key + "\t" + value;
    System.out.println("[IT-METRIC] " + line);
    var file = setting("armonik.it.metrics", "ARMONIK_IT_METRICS", "target/it-metrics-" + label + ".tsv");
    try {
      var path = Paths.get(file);
      if (path.getParent() != null) Files.createDirectories(path.getParent());
      Files.writeString(path, line + System.lineSeparator(), StandardOpenOption.CREATE, StandardOpenOption.APPEND);
    } catch (IOException e) {
      System.out.println("Could not record the metric: " + e);
    }
  }

  // ---------------------------------------------------------------- listener

  /** Records what the SDK reports, and on which threads. */
  public static class RecordingListener implements BlobCompletionListener {
    public final Map<BlobId, byte[]> data = new ConcurrentHashMap<>();
    public final Map<BlobId, Throwable> errors = new ConcurrentHashMap<>();
    public final AtomicInteger successes = new AtomicInteger();
    public final AtomicInteger failures = new AtomicInteger();
    public final AtomicLong lastEventNanos = new AtomicLong(System.nanoTime());
    public final Set<String> threads = ConcurrentHashMap.newKeySet();

    @Override
    public void onSuccess(Blob blob) {
      threads.add(Thread.currentThread().getName().replaceAll("\\d+", "N"));
      data.put(blob.blobInfo().id(), blob.data());
      successes.incrementAndGet();
      lastEventNanos.set(System.nanoTime());
    }

    @Override
    public void onError(BlobError blobError) {
      threads.add(Thread.currentThread().getName().replaceAll("\\d+", "N"));
      errors.put(blobError.blobInfo().id(), blobError.cause());
      failures.incrementAndGet();
      lastEventNanos.set(System.nanoTime());
    }

    public int events() {
      return successes.get() + failures.get();
    }
  }
}
