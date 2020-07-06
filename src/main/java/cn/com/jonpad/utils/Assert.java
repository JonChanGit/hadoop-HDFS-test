package cn.com.jonpad.utils;

import javax.annotation.Nullable;

/**
 * @author Jon Chan
 * @date 2020/7/6 23:38
 */
public abstract class Assert {
  /**
   * Check whether the given {@code String} is empty.
   * <p>This method accepts any Object as an argument, comparing it to
   * {@code null} and the empty String. As a consequence, this method
   * will never return {@code true} for a non-null non-String object.
   * <p>The Object signature is useful for general attribute handling code
   * that commonly deals with Strings but generally has to iterate over
   * Objects since attributes may e.g. be primitive value objects as well.
   * @param str the candidate String
   * @since 3.2.1
   */
  public static boolean isEmpty(@Nullable Object str) {
    return (str == null || "".equals(str));
  }
}
