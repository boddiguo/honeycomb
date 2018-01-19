package io.honeycomb.common.exception;

import java.io.PrintWriter;
import java.io.StringWriter;

/**
 * Created by guoyubo on 2018/1/10.
 */
public class HoneyCombException extends RuntimeException {

  private static final long serialVersionUID = 1L;

  private ErrorCode errorCode;

  public HoneyCombException(ErrorCode errorCode, String errorMessage) {
    super(errorCode.toString() + " - " + errorMessage);
    this.errorCode = errorCode;
  }

  public HoneyCombException(final String message, final Throwable cause) {
    super(message, cause);
  }

  public HoneyCombException(ErrorCode errorCode, String errorMessage, Throwable cause) {
    super(errorCode.toString() + " - " + getMessage(errorMessage) + " - " + getMessage(cause), cause);

    this.errorCode = errorCode;
  }

  public HoneyCombException(final CommonErrorCode errorCode, final Exception cause) {
    super(cause);
    this.errorCode = errorCode;
  }

  public HoneyCombException(final String message) {
    super(message);
  }

  public ErrorCode getErrorCode() {
    return this.errorCode;
  }

  private static String getMessage(Object obj) {
    if (obj == null) {
      return "";
    }

    if (obj instanceof Throwable) {
      StringWriter str = new StringWriter();
      PrintWriter pw = new PrintWriter(str);
      ((Throwable) obj).printStackTrace(pw);
      return str.toString();
      // return ((Throwable) obj).getMessage();
    } else {
      return obj.toString();
    }
  }
}
