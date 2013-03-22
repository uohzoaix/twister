/*
 * Copyright 2012 youku http://www.youku.com
 */
package com.twister.utils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.Charset;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author guoqing
 * 
 */

public final class Common {
	public static final String RealLogEntryPattern = "^([\\d.]+) \"(\\d{4}\\-\\d{2}\\-\\d{2}T\\d{2}:\\d{2}:\\d{2}\\+\\d{2}:\\d{2})\" (\\S+) \"(\\S+)\" \"(.+)\" \"(\\S*)\" ([\\d]+) ([\\d]+) ([\\d]+\\.[\\d]+) \"(.*)\"\\s{0,1}(.*)";
	public static Pattern RealLogPattern = Pattern.compile(RealLogEntryPattern);
	// syslog-ng add per
	public static final Pattern syslogExtPer = Pattern
			.compile("^(\\w+\\s+\\d+\\s+\\d{2}:\\d{2}:\\d{2})\\s+\\w([a-zA-Z_0-9\\-]+) ");
	public static final Pattern Ipv4 = Pattern.compile("\\d+\\.\\d+\\.\\d+\\.\\d+");
	public static final Pattern Ipv6 = Pattern.compile("\\S*:\\S*:\\S*:\\S*:\\S*:\\S*:\\S*:\\S*:");
	public static final Pattern DateStrPat = Pattern
			.compile("(\\d{4})\\-(\\d{2})\\-(\\d{2})\\s{1}(\\d{2}):(\\d{2}):(\\d{2})");
	public static final Pattern Digit = Pattern.compile("\\d+");
	public static final Pattern Alpha = Pattern.compile("[a-zA-Z]+");
	// 只要特殊的uri包括变量的才去执行正则表达式匹配
	public static final Pattern SpecialRegex = Pattern.compile("videos|search|shows|user|channels");
	
	public static String UriRegexFile = "conf/uriRegex.conf"; // target uri
	public static String UriRegexHDFSFile = "/workspace/mobile/Statis/mapi/conf/uriRegex.conf"; // on
																								// hdfs
	public static String TudoUriRegexFile = "conf/tudoUriRegex.conf"; // inclue
																		// jar
	public static String TudoUriRegexHDFSFile = "/workspace/mobile/Statis/mapi/conf/tudoUriRegex.conf"; // on
																										// hdfs
	public static String YoukuUriRegexFile = "conf/youkuUriRegex.conf"; // inclue
																		// jar
	public static String YoukuUriRegexHDFSFile = "/workspace/mobile/Statis/mapi/conf/youkuUriRegex.conf"; // on
																											// hdfs
	
	public static long timeunit = 1000l;
	public static long TMIN = 15; // t<15ms,优秀
	public static long TB = 60; // 15ms≤t<60ms ,良好
	public static long TC = 3000; // 60ms≤t<3000ms ,达标
	public static long TD = 6000; // 3000ms≤t<6000ms ,慢
	public static long TMAX = 6000; // t≥6000ms,超时
	
	/**
	 * 优秀(t<15ms) 良好(15≤t<60ms) 达标(60≤t<3000ms) 及慢(3000≤t<6000ms) 超时(t≥6000ms)
	 * 异常(cnterr)
	 */
	
	/**
	 * trim start /openapi-wireless and end .json .text .xml </p>
	 * 
	 * @return String uri
	 */
	public static String TrimUri(String uri) {
		if (uri.startsWith("/openapi-wireless")) {
			uri = uri.replace("/openapi-wireless", "");
		}
		if (uri.endsWith(".text")) {
			uri = uri.replace(".text", "");
		}
		if (uri.endsWith(".json")) {
			uri = uri.replace(".json", "");
		}
		if (uri.endsWith(".xml")) {
			uri = uri.replace(".xml", "");
		}
		if (uri == null) {
			uri = "";
		}
		try {
			uri = new String(uri.toString().getBytes(), "UTF-8");
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		}
		return uri;
		
	}
	
	public static int[] get_ip4suffix() {
		int ser[] = { 38, 39, 40, 73, 74, 75, 77, 78, 79, 80, 86, 87, 88, 89, 90, 142, 161, 62, 163 };
		return ser;
	}
	
	public static boolean checkFile(String filename) {
		boolean cf = false;
		try {
			URL url = Common.class.getClassLoader().getResource(filename);
			File file = new File(url.toURI());
			int size = 0;
			Boolean ckf = file.exists();
			@SuppressWarnings("resource")
			FileInputStream tmpfis = new FileInputStream(file);
			size = tmpfis.available();
			cf = (ckf && size > 0);
		} catch (FileNotFoundException e2) {
			e2.printStackTrace();
		} catch (IOException e1) {
			e1.printStackTrace();
		} catch (URISyntaxException e) {
			e.printStackTrace();
		}
		return cf;
	}
	
	public static ArrayList<Map<String, Serializable>> getUriRegexConf(String filename) {
		System.out.println("Common.getUriRegexConf: " + filename);
		ArrayList<Map<String, Serializable>> reglist = new ArrayList<Map<String, Serializable>>();
		try {
			boolean ck = Common.checkFile(UriRegexFile);
			if (!ck) {
				System.out.println("checkFile FileNotFound " + filename);
				throw new FileNotFoundException();
			}
			BufferedReader br = new BufferedReader(new InputStreamReader(Common.class.getClassLoader()
					.getResourceAsStream(filename)));
			String line = null;
			while ((line = br.readLine()) != null) {
				line = new String(line.getBytes(), Charset.forName("UTF-8")); // 编码转换
				if (line.startsWith("#")) {
					continue;
				}
				String val[] = line.split("\t");
				String meth = val[0];
				String lable = val[1];
				String regex = val[2];
				if (!regex.startsWith("^")) {
					regex = "^" + regex;
				}
				if (!regex.endsWith("$")) {
					regex = regex + "$";
				}
				String groups = "";
				if (val.length > 3) {
					groups = val[3];
				}
				String rely = "0";
				if (val.length > 4) {
					rely = val[4];
				}
				
				Pattern p = Pattern.compile(regex, Pattern.CASE_INSENSITIVE);
				String gp[] = groups.split(",");
				
				regex = StrEscape(regex);
				@SuppressWarnings("unused")
				String tmpv = "" + meth + "\t" + lable + "\t" + regex + "\t" + groups + "\t" + rely;
				// System.out.println(tmpv);
				Map<String, Serializable> map = new HashMap<String, Serializable>();
				map.put("method", meth.toUpperCase());
				map.put("lable", lable);
				map.put("pattern", p);
				map.put("groups", gp);
				map.put("rely", rely);
				reglist.add(map);
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return reglist;
	}
	
	/**
	 * deleteFile or dir file </p>
	 * 
	 * @return deleteFile
	 */
	public static void deleteFile(final File file) {
		if (file.exists()) {
			if (file.isFile()) {
				file.delete();
				return;
			} else if (file.isDirectory()) {
				File files[] = file.listFiles();
				for (int i = 0; i < files.length; i++) {
					deleteFile(files[i]);
				}
			}
			file.delete();
		}
		
	}
	
	public static String[] arraytoStringlist(ArrayList<String> list) {
		String itr[] = new String[list.size()];
		for (int i = 0; i < list.size(); i++) {
			itr[i] = list.get(i);
		}
		return itr;
	}
	
	public static ArrayList<String> stringtoArraylist(String[] itr) {
		ArrayList<String> buf = new ArrayList<String>();
		for (int i = 0; i < itr.length; i++) {
			buf.add(itr[i]);
		}
		return buf;
	}
	
	public static String get_file_date(String filename) {
		String str = "00000000";
		Pattern pat = Pattern.compile("(\\d{8})");
		Matcher pm = pat.matcher(filename);
		if (pm.find()) {
			str = pm.group(1);
		}
		return str;
	}
	
	public static String get_server(String filename) {
		String str = "00";
		ArrayList<String> ser = stringtoArraylist(filename.split("_"));
		int pos = ser.size() - 1;
		if (ser != null && pos > 0) {
			String tmp = ser.get(ser.size() - 1);
			tmp = tmp.trim();
			if (tmp.contains(".")) {
				tmp = tmp.replaceAll(".\\w*$", "");
			}
			Pattern pat = Pattern.compile("^(\\d+)$");
			Matcher pm = pat.matcher(tmp);
			if (pm.matches()) {
				// from file name
				str = pm.group(1);
			}
		}
		return str;
	}
	
	/**
	 * //转义\'"三个字符 </p>
	 * 
	 * @return String
	 */
	public static String StrEscape(String str) {
		
		String result = "";
		char[] chS = str.toCharArray();
		for (int i = 0; i < chS.length; i++) {
			if (chS[i] == '"' || chS[i] == '\'' || chS[i] == '\\')
				result += "\\";
			result += chS[i];
		}
		return result;
	}
	
	public static boolean checkAccessLog(String vline) {
		if (vline.contains("GET") || vline.contains("POST")) {
			return true;
		} else {
			return false;
		}
	}
	
	/**
	 * 正则匹配uri </p>
	 * 
	 * @return map={uri_name=/videos/vid/playurl, kls=VideoPlayurl,
	 *         uri=/videos/XMzg5MzYxMjYw/playurl, find=true} or
	 *         {uri_name=/videos/XMzg5MzYxMjYw/playurl, kls=other,
	 *         uri=/videos/XMzg5MzYxMjYw/playurl, find=false}
	 */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public static Map MatcherUri(ArrayList UriRegex, String uri, String method) {
		uri = Common.TrimUri(uri);
		
		HashMap rlt = new HashMap();
		rlt.put("find", false);
		rlt.put("kls", "other");
		rlt.put("uri", uri);
		rlt.put("uri_name", uri);
		rlt.put("rely", '0');
		for (int i = 0; i < UriRegex.size(); i++) {
			HashMap map = (HashMap) UriRegex.get(i);
			String lable = (String) map.get("lable");
			Pattern pattern = (Pattern) map.get("pattern");
			String[] gp = (String[]) map.get("groups");
			String meth = (String) map.get("method");
			String rely = (String) map.get("rely");
			
			meth = meth.toUpperCase();
			if (!meth.equals(method.toUpperCase())) {
				continue;
			}
			Matcher realMatcher = pattern.matcher(uri);
			if (realMatcher.matches()) {
				rlt.put("find", true);
				rlt.put("kls", lable);
				rlt.put("uri", uri);
				rlt.put("rely", rely);
				for (int k = 0; k < realMatcher.groupCount(); k++) {
					int x = k + 1;
					int y = k;
					String rexpv = realMatcher.group(x);
					if (gp.length > y && rexpv != null) {
						String reval = gp[y];
						uri = uri.replace(rexpv, reval);
					}
				}
				rlt.put("uri_name", uri);
				break;
			}
		}
		return rlt;
	}
	
	/**
	 * @param string
	 *            2012-06-13T00:01:14+08:00
	 * @return string 2012-06-13 00:01:14
	 */
	public static String formatDateStr(String date) {
		date = date.replace("T", " ");
		date = date.replace("+08:00", "");
		return date;
	}
	
	/**
	 * @param string
	 *            yyyy-MM-dd HH:mm:ss
	 * @return string yyyymmdd
	 */
	public static String dateString(String timestr) {
		
		try {
			SimpleDateFormat dateFormat1 = new SimpleDateFormat("yyyyMMdd");
			if (timestr.length() == 10) {
				SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
				Date d = dateFormat.parse(timestr);
				return dateFormat1.format(d);
			} else {
				SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
				Date d = dateFormat.parse(timestr);
				return dateFormat1.format(d);
			}
		} catch (ParseException e) {
		}
		return "";
	}
	
	/**
	 * @param string
	 *            yyyy-MM-dd HH:mm:ss
	 * @return Timestamp Timestamp
	 */
	public static Timestamp dateTimestamp(String timestr) {
		Timestamp t = null;
		try {
			
			SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			t = new Timestamp(dateFormat.parse(timestr).getTime());
		} catch (ParseException e) {
		}
		
		return t;
	}
	
	public static String format_ua3(String user_agent) {
		/*
		 * get [Youku产品;产品版本;平台] stand ua examples
		 * Youku;2.3.1;Android;2.3.5;GT-I9003
		 */
		String ukey = "unknown;0;0";
		String[] uav = user_agent.split(";");
		if (uav.length == 5) {
			ukey = uav[0] + ";" + uav[1] + ";" + uav[2] + ";";
		} else {
			if (uav.length >= 3) {
				ukey = uav[0] + ";0;0;";
			} else {
				ukey = "unknown;0;0";
			}
		}
		return ukey;
		
	}
	
	/**
	 * @param string
	 *            yyyy-MM-dd HH:mm:ss
	 * @return Long times 毫秒
	 */
	public static Long dateLong(String timestr) {
		Long t = 0l;
		try {
			
			SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			t = dateFormat.parse(timestr).getTime();
			
		} catch (ParseException e) {
		}
		return t;
	}
	
	/**
	 * @param long time 毫秒
	 * 
	 * @return string yyyy-MM-dd HH:mm:ss
	 */
	public static String longDateStr(long time) {
		String dt = "";
		try {
			SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			dt = dateFormat.format(new Date(time));
			
		} catch (Exception e) {
		}
		return dt;
	}
	
	/**
	 * 将long毫秒转成long分，抛弃秒值
	 * 
	 * @param long time 毫秒 *
	 * @return string yyyy-MM-dd HH:mm:ss
	 */
	public static Long longMinute(String timestr) {
		Long t = 0l;
		try {
			
			SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm");
			t = dateFormat.parse(timestr).getTime();
		} catch (ParseException e) {
		}
		return t;
	}
	
	/**
	 * 抛弃分秒值
	 * 
	 * @param ldatetimestr
	 *            2013-03-03 10:03:14
	 * @return string 20130318#10:03:00
	 */
	public static String formatDataTimeStr2(String datetimestr) {
		String dt = "";
		try {
			Matcher pm = DateStrPat.matcher(datetimestr);
			if (pm.matches()) {
				dt = String.format("%s%s%s#%s:00:00", pm.group(1), pm.group(2), pm.group(3), pm.group(4));
			}
			
		} catch (Exception e) {
		}
		return dt;
	}
	
	/**
	 * 抛弃秒值
	 * 
	 * @param ldatetimestr
	 *            2013-03-03 10:03:14
	 * @return string 20130318#10:03:00
	 */
	public static String formatDataTimeStr1(String datetimestr) {
		String dt = "";
		try {
			Matcher pm = DateStrPat.matcher(datetimestr);
			if (pm.matches()) {
				dt = String.format("%s%s%s#%s:%s:00", pm.group(1), pm.group(2), pm.group(3), pm.group(4), pm.group(5));
			}
			
		} catch (Exception e) {
		}
		return dt;
	}
	
	/**
	 * 
	 * @param ldatetimestr
	 *            2013-03-03 10:03:14
	 * @return string 20130318#10:03:03
	 */
	public static String formatDataTimeStr(String datetimestr) {
		String dt = "";
		try {
			Matcher pm = DateStrPat.matcher(datetimestr);
			if (pm.matches()) {
				dt = String.format("%s%s%s#%s:%s:%s", pm.group(1), pm.group(2), pm.group(3), pm.group(4), pm.group(5),
						pm.group(6));
			}
			
		} catch (Exception e) {
		}
		return dt;
	}
	
	public static int validate_Response_code(String response_code) {
		int v = Integer.parseInt(response_code);
		int code_flag = 0;
		if (v == 200 || v == 206 || v == 301 || v == 302 || v == 304) {
			code_flag = 1;
		}
		return code_flag;
	}
	
	/**
	 * 提据resp code 和resp time 评估接口性能,(异常,优秀,良好,达标,不达标,超时 )
	 * 
	 * @param response_code
	 * @param request_time
	 * @return list<long> (异常,优秀,良好,达标,不达标,超时 )
	 * 
	 */
	public static int[] assess_request_time(int response_code, long request_time) {
		// requesttime
		// 优秀 良好 达标 超时 异常
		int[] art = { 0, 0, 0, 0, 0, 0 };
		if (Common.validate_Response_code(String.valueOf(response_code)) != 1) {
			// cnterr=1 异常
			art[0] = 1;
		} else {
			if (request_time > 0) {
				// t＜15ms,优；15ms≤t<60ms ,良好； 60ms≤t<3000ms ,达标 ； 3000ms≤t<6000ms
				// ,慢； t≥6000ms,超时； cnterr=1 异常
				if (request_time < Common.TMIN) {
					// t<15ms,优秀
					art[1] = 1;
				} else if (request_time >= Common.TMIN && request_time < Common.TB) {
					// 15ms≤t<60ms ,良好
					art[2] = 1;
				} else if (request_time >= Common.TB && request_time < Common.TC) {
					// 60ms≤t<3000ms ,达标
					art[3] = 1;
				} else if (request_time >= Common.TC && request_time < Common.TMAX) {
					// 3000ms≤t ,不达标，慢
					art[4] = 1;
				} else if (request_time > Common.TMAX) {
					// t≥6000ms,超时
					art[5] = 1;
				}
			}
		}
		return art;
	}
	
	/**
	 * 提供精确的小数位四舍五入处理。
	 * 
	 * @param v
	 *            需要四舍五入的数字
	 * @param scale
	 *            小数点后保留几位
	 * @return 四舍五入后的结果
	 */
	public static double doubleRound(double v, int scale) {
		if (scale < 0) {
			scale = 0;
		}
		BigDecimal b = new BigDecimal(0);
		try {
			b = new BigDecimal(String.valueOf(v));
		} catch (Exception e) {
			return v;
		}
		BigDecimal one = new BigDecimal("1");
		return b.divide(one, scale, BigDecimal.ROUND_HALF_UP).doubleValue();
	}
	
	public static BigDecimal bigDecimalRound(BigDecimal v, int scale) {
		if (scale < 0) {
			scale = 0;
		}
		BigDecimal one = new BigDecimal("1");
		return v.divide(one, scale, BigDecimal.ROUND_HALF_UP);
	}
	
	public static Map<String, String[]> server_config() {
		Map<String, String[]> cfg = new HashMap<String, String[]>();
		// 序号 主机名 内网IP 外网IP 域名
		cfg.put("00", new String[] { "a00.api.3g.b28.youku", "00.00.00.00", "00.00.00.00", "api.3g.youku.com" });
		cfg.put("01", new String[] { "a01.api.3g.b28.youku", "10.103.13.38", "211.151.50.150", "api.3g.youku.com" });
		cfg.put("02", new String[] { "a02.api.3g.b28.youku", "10.103.13.39", "211.151.50.151", "api.3g.youku.com" });
		cfg.put("03", new String[] { "a03.api.3g.b28.youku", "10.103.13.73", "211.151.50.176", "api.3g.youku.com" });
		cfg.put("04", new String[] { "a04.api.3g.b28.youku", "10.103.13.74", "211.151.50.177", "api.3g.youku.com" });
		cfg.put("05", new String[] { "a05.api.3g.b29.youku", "10.103.13.75", "211.151.50.178", "api.3g.youku.com" });
		cfg.put("06", new String[] { "a06.api.3g.b28.youku", "10.103.13.40", "211.151.50.152", "api.3g.youku.com" });
		cfg.put("07", new String[] { "a07.api.3g.b28.youku", "10.103.13.77", "211.151.50.183", "api.3g.youku.com" });
		cfg.put("08", new String[] { "a08.api.3g.b28.youku", "10.103.13.78", "211.151.50.184", "api.3g.youku.com" });
		cfg.put("09", new String[] { "a09.api.3g.b28.youku", "10.103.13.79", "211.151.50.185", "api.3g.youku.com" });
		cfg.put("10", new String[] { "a10.api.3g.b28.youku", "10.103.13.80", "211.151.50.186", "api.3g.youku.com" });
		cfg.put("11", new String[] { "a11.api.3g.b28.youku", "10.103.13.86", "211.151.50.187", "api.3g.youku.com" });
		cfg.put("12", new String[] { "a12.api.3g.b28.youku", "10.103.13.87", "211.151.50.188", "api.3g.youku.com" });
		cfg.put("13", new String[] { "a13.api.3g.b28.youku", "10.103.13.88", "211.151.50.189", "api.3g.youku.com" });
		cfg.put("14", new String[] { "a14.api.3g.b28.youku", "10.103.13.89", "211.151.50.190", "api.3g.youku.com" });
		cfg.put("15", new String[] { "a15.api.3g.b28.youku", "10.103.13.90", "211.151.50.191", "api.3g.youku.com" });
		cfg.put("16", new String[] { "a16.api.3g.b28.youku", "10.103.13.161", "211.151.50.192", "api.3g.youku.com" });
		cfg.put("17", new String[] { "a17.api.3g.b28.youku", "10.103.13.162", "211.151.50.193", "api.3g.youku.com" });
		cfg.put("18", new String[] { "a18.api.3g.b28.youku", "10.103.13.163", "211.151.50.194", "api.3g.youku.com" });
		return cfg;
		
	}
	
	public static Map<String, String[]> tudo_server_config() {
		Map<String, String[]> cfg = new HashMap<String, String[]>();
		// 序号 主机名 内网IP 外网IP 域名
		cfg.put("00", new String[] { "a00.api.3g.b28.youku", "10.103.13.18", "00.00.00.00", "api.3g.youku.com" });
		cfg.put("01", new String[] { "a01-api-3g-b28-tudou", "10.103.28.81", "211.151.50.217", "a01.api.3g.b28.tudou" });
		cfg.put("02", new String[] { "a02-api-3g-b28-tudou", "10.103.28.82", "211.151.50.151", "a02.api.3g.b28.tudou" });
		
		return cfg;
		
	}
	
	public static void main(String[] args) {
		
		String str = "/openapi-wireless/videos/XMzg5MzYxMjYw/playurl.json";
		ArrayList<Map<String, Serializable>> ur = Common.getUriRegexConf(Common.UriRegexFile);
		HashMap m = (HashMap) MatcherUri(ur, str, "GET");
		System.out.println(formatDataTimeStr("2012-06-13 10:01:14"));
		System.out.println(m);
		
	}
	
}
