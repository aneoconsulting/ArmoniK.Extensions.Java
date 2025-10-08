package fr.aneo.armonik.worker;

sealed public interface TaskOutcome {
  record Success() implements TaskOutcome {}
  record Error(String message) implements TaskOutcome {}
}
