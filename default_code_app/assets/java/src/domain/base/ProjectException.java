package domain.base;

import java.util.HashMap;
import java.util.Map;

public class ProjectException extends RuntimeException {

  private static final long serialVersionUID = -3929475919873279157L;

  private ErrorCode code;

  public ProjectException(ErrorCode code) {
    super(code.getDefaultMessage());
    this.code = code;
  }

  public ProjectException(ErrorCode code, String message) {
    super(message);
    this.code = code;
  }

  public ProjectException(ErrorCode code, Throwable cause) {
    super(code.getDefaultMessage(), cause);
    this.code = code;
  }

  public ProjectException(ErrorCode code, String message, Throwable cause) {
    super(message, cause);
    this.code = code;
  }

  public Map<String, Object> getAsMap() {
    Map<String, Object> result = new HashMap<>();
    result.put("code", code.getCode());
    result.put("message", getMessage());
    return result;
  }
}
