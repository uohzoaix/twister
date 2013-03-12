package com.twister.utils.internal;

import java.util.ArrayList;
import java.util.Formatter;
import java.util.List;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
 

/**
 * 字符串工具类，用于实现一些字符串的常用操作
 * <ul>
 * <li>继承自{@link org.apache.commons.lang3.StringUtils}</li>
 * </ul>
 * 
 
 */
/**
 * String utility class.
 */
public final class StringUtils extends org.apache.commons.lang3.StringUtils {
	
	private StringUtils() {
		// Unused.
	}
	
	public static final String NEWLINE;
	
	static {
		String newLine;
		
		try {
			newLine = new Formatter().format("%n").toString();
		} catch (Exception e) {
			newLine = "\n";
		}
		
		NEWLINE = newLine;
	} 

    /**
     * 比较两个String
     * 
     * @param actual
     * @param expected
     * @return
     *         <ul>
     *         <li>若两个字符串都为null，则返回true</li>
     *         <li>若两个字符串都不为null，且相等，则返回true</li>
     *         <li>否则返回false</li>
     *         </ul>
     */
    public static boolean isEquals(String actual, String expected) {
        return ObjectUtils.isEquals(actual, expected);
    }

    /**
     * null字符串转换为长度为0的字符串
     * 
     * @param str 待转换字符串
     * @return
     * @see
     * <pre>
     *  nullStrToEmpty(null)    =   "";
     *  nullStrToEmpty("")      =   "";
     *  nullStrToEmpty("aa")    =   "aa";
     * </pre>
     */
    public static String nullStrToEmpty(String str) {
        return (str == null ? "" : str);
    }

    /**
     * 将字符串首字母大写后返回
     * 
     * @param str 原字符串
     * @return 首字母大写后的字符串
     * 
     * <pre>
     *      capitalizeFirstLetter(null)     =   null;
     *      capitalizeFirstLetter("")       =   "";
     *      capitalizeFirstLetter("2ab")    =   "2ab"
     *      capitalizeFirstLetter("a")      =   "A"
     *      capitalizeFirstLetter("ab")     =   "Ab"
     *      capitalizeFirstLetter("Abc")    =   "Abc"
     * </pre>
     */
    public static String capitalizeFirstLetter(String str) {
        if (isEmpty(str)) {
            return str;
        }

        char c = str.charAt(0);
        return (!Character.isLetter(c) || Character.isUpperCase(c)) ? str : new StringBuilder(str.length()).append(Character.toUpperCase(c)).append(str.substring(1)).toString();
    }

    /**
     * 如果不是普通字符，则按照utf8进行编码
     * 
     * <pre>
     * utf8Encode(null)        =   null
     * utf8Encode("")          =   "";
     * utf8Encode("aa")        =   "aa";
     * utf8Encode("啊啊啊啊")   = "%E5%95%8A%E5%95%8A%E5%95%8A%E5%95%8A";
     * </pre>
     * 
     * @param str 原字符
     * @return 编码后字符，若编码异常抛出异常
     */
    public static String utf8Encode(String str) {
        if (!isEmpty(str) && str.getBytes().length != str.length()) {
            try {
                return URLEncoder.encode(str, "UTF-8");
            } catch (UnsupportedEncodingException e) {
                throw new RuntimeException("UnsupportedEncodingException occurred. ", e);
            }
        }
        return str;
    }

    /**
     * 如果不是普通字符，则按照utf8进行编码，编码异常则返回defultReturn
     * 
     * @param str 源字符串
     * @param defultReturn 出现异常默认返回
     * @return
     */
    public static String utf8Encode(String str, String defultReturn) {
        if (!isEmpty(str) && str.getBytes().length != str.length()) {
            try {
                return URLEncoder.encode(str, "UTF-8");
            } catch (UnsupportedEncodingException e) {
                return defultReturn;
            }
        }
        return str;
    }

    /**
     * 得到href链接的innerHtml
     * 
     * @param href href内容
     * @return href的innerHtml
     *         <ul>
     *         <li>空字符串返回""</li>
     *         <li>若字符串不为空，且不符合链接正则的返回原内容</li>
     *         <li>若字符串不为空，且符合链接正则的返回最后一个innerHtml</li>
     *         </ul>
     * @see
     * <pre>
     *      getHrefInnerHtml(null)                                  = ""
     *      getHrefInnerHtml("")                                    = ""
     *      getHrefInnerHtml("mp3")                                 = "mp3";
     *      getHrefInnerHtml("&lt;a innerHtml&lt;/a&gt;")                    = "&lt;a innerHtml&lt;/a&gt;";
     *      getHrefInnerHtml("&lt;a&gt;innerHtml&lt;/a&gt;")                    = "innerHtml";
     *      getHrefInnerHtml("&lt;a&lt;a&gt;innerHtml&lt;/a&gt;")                    = "innerHtml";
     *      getHrefInnerHtml("&lt;a href="baidu.com"&gt;innerHtml&lt;/a&gt;")               = "innerHtml";
     *      getHrefInnerHtml("&lt;a href="baidu.com" title="baidu"&gt;innerHtml&lt;/a&gt;") = "innerHtml";
     *      getHrefInnerHtml("   &lt;a&gt;innerHtml&lt;/a&gt;  ")                           = "innerHtml";
     *      getHrefInnerHtml("&lt;a&gt;innerHtml&lt;/a&gt;&lt;/a&gt;")                      = "innerHtml";
     *      getHrefInnerHtml("jack&lt;a&gt;innerHtml&lt;/a&gt;&lt;/a&gt;")                  = "innerHtml";
     *      getHrefInnerHtml("&lt;a&gt;innerHtml1&lt;/a&gt;&lt;a&gt;innerHtml2&lt;/a&gt;")        = "innerHtml2";
     * </pre>
     */
    public static String getHrefInnerHtml(String href) {
        if (isEmpty(href)) {
            return "";
        }
        String hrefReg = ".*<[\\s]*a[\\s]*.*>(.+?)<[\\s]*/a[\\s]*>.*";
        Pattern hrefPattern = Pattern.compile(hrefReg, Pattern.CASE_INSENSITIVE);
        Matcher hrefMatcher = hrefPattern.matcher(href);
        if (hrefMatcher.matches()) {
            return hrefMatcher.group(1);
        }
        return href;
    }

/**
     * html的转义字符转换成正常的字符串
     * 
     * <pre>
     * htmlEscapeCharsToString(null) = null;
     * htmlEscapeCharsToString("") = "";
     * htmlEscapeCharsToString("mp3") = "mp3";
     * htmlEscapeCharsToString("mp3&lt;") = "mp3<";
     * htmlEscapeCharsToString("mp3&gt;") = "mp3\>";
     * htmlEscapeCharsToString("mp3&amp;mp4") = "mp3&mp4";
     * htmlEscapeCharsToString("mp3&quot;mp4") = "mp3\"mp4";
     * htmlEscapeCharsToString("mp3&lt;&gt;&amp;&quot;mp4") = "mp3\<\>&\"mp4";
     * </pre>
     * 
     * @param source
     * @return
     */
    public static String htmlEscapeCharsToString(String source) {
        if (org.apache.commons.lang3.StringUtils.isEmpty(source)) {
            return source;
        } else {
            return source.replaceAll("&lt;", "<").replaceAll("&gt;", ">").replaceAll("&amp;", "&").replaceAll("&quot;",
                                                                                                              "\"");
        }
    }

    /**
     * 半角字符转换为全角字符
     * 
     * <pre>
     * fullWidthToHalfWidth(null) = null;
     * fullWidthToHalfWidth("") = "";
     * fullWidthToHalfWidth(new String(new char[] {12288})) = " ";
     * fullWidthToHalfWidth("！＂＃＄％＆) = "!\"#$%&";
     * </pre>
     * 
     * @param s
     * @return
     */
    public static String fullWidthToHalfWidth(String s) {
        if (isEmpty(s)) {
            return s;
        }

        char[] source = s.toCharArray();
        for (int i = 0; i < source.length; i++) {
            if (source[i] == 12288) {
                source[i] = ' ';
                // } else if (source[i] == 12290) {
                // source[i] = '.';
            } else if (source[i] >= 65281 && source[i] <= 65374) {
                source[i] = (char)(source[i] - 65248);
            } else {
                source[i] = source[i];
            }
        }
        return new String(source);
    }

    /**
     * 全角字符转换为半角字符
     * 
     * <pre>
     * halfWidthToFullWidth(null) = null;
     * halfWidthToFullWidth("") = "";
     * halfWidthToFullWidth(" ") = new String(new char[] {12288});
     * halfWidthToFullWidth("!\"#$%&) = "！＂＃＄％＆";
     * </pre>
     * 
     * @param s
     * @return
     */
    public static String halfWidthToFullWidth(String s) {
        if (isEmpty(s)) {
            return s;
        }

        char[] source = s.toCharArray();
        for (int i = 0; i < source.length; i++) {
            if (source[i] == ' ') {
                source[i] = (char)12288;
                // } else if (source[i] == '.') {
                // source[i] = (char)12290;
            } else if (source[i] >= 33 && source[i] <= 126) {
                source[i] = (char)(source[i] + 65248);
            } else {
                source[i] = source[i];
            }
        }
        return new String(source);
    }
 

	
	/**
	 * Strip an Object of it's ISO control characters.
	 * 
	 * @param value
	 *            The Object that should be stripped. This objects toString
	 *            method will called and the result passed to
	 *            {@link #stripControlCharacters(String)}.
	 * @return {@code String} A new String instance with its hexadecimal control
	 *         characters replaced by a space. Or the unmodified String if it
	 *         does not contain any ISO control characters.
	 */
	public static String stripControlCharacters(Object value) {
		if (value == null) {
			return null;
		}
		
		return stripControlCharacters(value.toString());
	}
	
	/**
	 * Strip a String of it's ISO control characters.
	 * 
	 * @param value
	 *            The String that should be stripped.
	 * @return {@code String} A new String instance with its hexadecimal control
	 *         characters replaced by a space. Or the unmodified String if it
	 *         does not contain any ISO control characters.
	 */
	public static String stripControlCharacters(String value) {
		if (value == null) {
			return null;
		}
		
		boolean hasControlChars = false;
		for (int i = value.length() - 1; i >= 0; i--) {
			if (Character.isISOControl(value.charAt(i))) {
				hasControlChars = true;
				break;
			}
		}
		
		if (!hasControlChars) {
			return value;
		}
		
		StringBuilder buf = new StringBuilder(value.length());
		int i = 0;
		
		// Skip initial control characters (i.e. left trim)
		for (; i < value.length(); i++) {
			if (!Character.isISOControl(value.charAt(i))) {
				break;
			}
		}
		
		// Copy non control characters and substitute control characters with
		// a space. The last control characters are trimmed.
		boolean suppressingControlChars = false;
		for (; i < value.length(); i++) {
			if (Character.isISOControl(value.charAt(i))) {
				suppressingControlChars = true;
				continue;
			} else {
				if (suppressingControlChars) {
					suppressingControlChars = false;
					buf.append(' ');
				}
				buf.append(value.charAt(i));
			}
		}
		
		return buf.toString();
	}
	
	private static final String EMPTY_STRING = "";
	
	/**
	 * Splits the specified {@link String} with the specified delimiter. This
	 * operation is a simplified and optimized version of
	 * {@link String#split(String)}.
	 */
	public static String[] split(String value, char delim) {
		final int end = value.length();
		final List<String> res = new ArrayList<String>();
		
		int start = 0;
		for (int i = 0; i < end; i++) {
			if (value.charAt(i) == delim) {
				if (start == i) {
					res.add(EMPTY_STRING);
				} else {
					res.add(value.substring(start, i));
				}
				start = i + 1;
			}
		}
		
		if (start == 0) { // If no delimiter was found in the value
			res.add(value);
		} else {
			if (start != end) {
				// Add the last element if it's not empty.
				res.add(value.substring(start, end));
			} else {
				// Truncate trailing empty elements.
				for (int i = res.size() - 1; i >= 0; i--) {
					if (res.get(i).length() == 0) {
						res.remove(i);
					} else {
						break;
					}
				}
			}
		}
		
		return res.toArray(new String[res.size()]);
	}
}
