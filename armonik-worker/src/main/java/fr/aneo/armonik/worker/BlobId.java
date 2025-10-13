package fr.aneo.armonik.worker;

import java.util.Objects;

public final class BlobId {
  private final String id;

  private BlobId(String id) {
    this.id = id;
  }

  static BlobId from(String id) {
    return new BlobId(id);
  }

  public String asString() {
    return id;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == this) return true;
    if (obj == null || obj.getClass() != this.getClass()) return false;
    var that = (BlobId) obj;
    return Objects.equals(this.id, that.id);
  }

  @Override
  public int hashCode() {
    return Objects.hash(id);
  }

  @Override
  public String toString() {
    return "BlobId[" +
      "id=" + id + ']';
  }
}
