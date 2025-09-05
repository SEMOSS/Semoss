package domain.base;

public enum ErrorCode {
  BAD_REQUEST(400, "Invalid request"),
  FORBIDDEN(403, "User is unauthorized to perform this operation"),
  NOT_FOUND(404, "Resource not found"),
  CONFLICT(409, "Conflicting resource update found"),
  INTERNAL_SERVER_ERROR(500, "Error during operation");

  private final int code;
  private final String defaultMessage;

  private ErrorCode(int code, String defaultMessage) {
    this.code = code;
    this.defaultMessage = defaultMessage;
  }

  public int getCode() {
    return code;
  }

  public String getDefaultMessage() {
    return defaultMessage;
  }

  public static ErrorCode fromCode(int code) {
    for (ErrorCode ec : ErrorCode.values()) {
      if (ec.getCode() == code) {
        return ec;
      }
    }
    return INTERNAL_SERVER_ERROR; // Default if no match found
  }
}
