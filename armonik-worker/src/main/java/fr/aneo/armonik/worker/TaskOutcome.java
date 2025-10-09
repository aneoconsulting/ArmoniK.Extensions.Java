package fr.aneo.armonik.worker;

public sealed interface TaskOutcome {
  record Success() implements TaskOutcome {}
  record Error(String message) implements TaskOutcome {}
}
