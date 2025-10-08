package fr.aneo.armonik.worker;

@FunctionalInterface
public interface TaskProcessor {

  TaskOutcome processTask(TaskHandler taskHandler);
}
