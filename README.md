# ArmoniK.Extensions.Java

Java SDK for [ArmoniK](https://github.com/aneoconsulting/ArmoniK), an open-source distributed computing platform. This repository provides the Java libraries for writing **clients** that submit work to ArmoniK and **workers** that execute it.

## Client–Worker model

ArmoniK follows a client–worker model with a clear separation of responsibilities:

- **Clients** create and manage sessions, submit tasks and their dependencies as a task graph, and manage input/output blobs.
- **Workers** implement the computation logic for a task, read its inputs, produce its outputs, and may submit further tasks to build dynamic workflows.

Clients and workers are interoperable across ArmoniK's supported languages: a Java client can drive tasks executed by C++ or C# workers, and vice versa.

## Modules

| Module | Artifact | Purpose |
|---|---|---|
| [`armonik-client`](armonik-client) | `fr.aneo:armonik-client` | Client library: create sessions, submit tasks, manage blobs, retrieve results. |
| [`worker/armonik-worker-domain`](worker/armonik-worker-domain) | `fr.aneo:armonik-worker-domain` | Domain model and `TaskProcessor` interface for implementing task logic, used for dynamic loading. |
| [`worker/armonik-worker`](worker/armonik-worker) | `fr.aneo:armonik-worker` | Worker runtime (`ArmoniKWorker`) for building statically deployed workers. |

`armonik-client` and `worker` are independent Maven builds, each with its own Maven Wrapper.

## Building

Requires JDK 17. From each module directory:

```bash
./mvnw verify
```

## Client example

```java
try (ArmoniKClient client = new ArmoniKClient(config)) {
    SessionHandle session = client.createSession(sessionDefinition);

    var taskDef = new TaskDefinition()
        .withInput("A", InputBlobDefinition.from("1".getBytes(UTF_8)))
        .withOutput("result");

    TaskHandle task = session.submitTask(taskDef);
}
```

Tasks can be chained into a graph by passing one task's output `BlobHandle` as another task's input; ArmoniK schedules the consuming task only once the producing task has written that blob.

## Worker example

```java
public class MyProcessor implements TaskProcessor {
    @Override
    public TaskOutcome processTask(TaskContext context) {
        TaskInput input = context.getInput("A");
        context.getOutput("result").write(input.rawData());
        return TaskOutcome.SUCCESS;
    }
}
```

A worker can run in **static mode** (custom Docker image with the processor embedded) or **dynamic mode** (pre-built image that loads a `TaskProcessor` JAR at runtime, referenced via `WorkerLibrary`).

## Documentation

Full documentation, including detailed guides on the client SDK, the worker SDK, and end-to-end examples, lives under [`.docs/content`](.docs/content) and is published to [Read the Docs](https://armonikextensionsjava.readthedocs.io/en/latest/). Runnable sample projects are maintained in the [ArmoniK.Samples](https://github.com/aneoconsulting/ArmoniK.Samples) repository.

## License

Licensed under the [Apache License, Version 2.0](LICENSE).
