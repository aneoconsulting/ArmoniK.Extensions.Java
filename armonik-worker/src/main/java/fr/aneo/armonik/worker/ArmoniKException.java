package fr.aneo.armonik.worker;

public class ArmoniKException extends RuntimeException {
  public ArmoniKException(String message) {
    super(message);
  }
  public ArmoniKException(String message, Throwable cause) {
    super(message,  cause);
  }
}
