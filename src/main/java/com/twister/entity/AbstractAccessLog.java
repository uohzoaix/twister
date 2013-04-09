package com.twister.entity;

import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.net.URLDecoder;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.twister.utils.AppsConfig;
import com.twister.utils.Common;
import com.twister.utils.JacksonUtils;

@JsonIgnoreProperties(value = { "logger", "LOGR", "serialVersionUID", "proxyIp", "ipInt", "request_args",
		"timeCalendar", "timeDate" })
public abstract class AbstractAccessLog<T> implements Serializable, IAccessLog<T> {
	
	@JsonIgnore
	private static final long serialVersionUID = 7308710264744648037L;
	
	@JsonIgnore
	private static Logger LOGR = LoggerFactory.getLogger(AbstractAccessLog.class);
	
	// fields: 0=api,1=jiekou,2=bingfa
	private int logVer = 0;
	private String ip = "";
	// 毫秒
	private long time = 0L;
	private String date_time = "";
	private String method = "";
	private String uri = "";
	private String request_args = "";
	private String request_body = "";
	private int response_code = 0;
	private long content_length = 0l; // 总流量kb
	private long request_time = 0l; // 响应ms
	private String user_agent = "";
	private String server = "00";
	// ext base args pid, match uri_name klsname
	private String pid = "";
	private String kls = "other";
	private String uri_name = "other";
	private String prov = "0000000000";
	private String city = "0000000000";
	private String guid = "";
	private String rely = "0";
	@JsonIgnore
	private ArrayList<Map<String, Serializable>> uriRegex = new ArrayList<Map<String, Serializable>>();
	
	public AbstractAccessLog() {
	}
	
	public AbstractAccessLog(String line) {
		this.initSettings();
		ArrayList<String> alog = parseLog(line);
		this.logExpandsToObject(alog);
		
	}
	
	@Override
	public void initSettings() {
		this.uriRegex = IAccessLog.uriRegex;
	}
	
	@Override
	public boolean checkAccessLog(String line) {
		if (line.contains("GET") || line.contains("POST")) {
			return true;
		} else {
			return false;
		}
	}
	
	@Override
	public ArrayList<String> parseLog(String line) {
		ArrayList<String> vec = new ArrayList<String>();
		String srcline = line;
		if (this.checkAccessLog(line)) {
			vec = logMatcher(line);
			if (vec.size() == 0) {
				vec = logSplit(srcline, SPACE);
			}
			return vec;
		} else {
			return vec;
		}
	}
	
	@Override
	public ArrayList<String> logMatcher(String line) {
		ArrayList<String> vec = new ArrayList<String>();
		try {
			// default RealLogPattern
			String server = "00";
			String isTudou = "0";
			Matcher pm2 = syslogExtPer.matcher(line);
			Matcher ipv4 = Ipv4.matcher(line);
			String syslogper = "";
			// 从syslog增加的2列里取hostname序号
			if (pm2.find()) {
				syslogper = pm2.group();
				server = pm2.group(2);
				// get运维hostid
				int attudou = server.indexOf('-');
				if (attudou > 0) {
					// tudo ser
					server = server.substring(0, attudou);
				} else {
					server = server.replaceAll("[^\\d]+", "");
				}
				if (syslogper.contains("Tudou") || syslogper.contains("tudou")) {
					isTudou = "1";
				}
				
			}
			
			// 去掉运维加的2列,从ip开始算正试日志
			if (ipv4.find()) {
				line = line.substring(ipv4.start());
			} else {
				line = line.replaceAll(syslogExtPer.toString(), "");
				
			}
			
			// System.out.println(line);
			Matcher realMatcher = RealLogPattern.matcher(line);
			if (realMatcher.matches()) {
				vec.add(new String(realMatcher.group(1).getBytes(), charSet));
				vec.add(new String(realMatcher.group(2).getBytes(), charSet));
				vec.add(new String(realMatcher.group(3).getBytes(), charSet));
				vec.add(new String(realMatcher.group(4).getBytes(), charSet));
				vec.add(new String(realMatcher.group(5).getBytes(), charSet));
				vec.add(new String(realMatcher.group(6).getBytes(), charSet));
				vec.add(new String(realMatcher.group(7).getBytes(), charSet));
				vec.add(new String(realMatcher.group(8).getBytes(), charSet));
				vec.add(new String(realMatcher.group(9).getBytes(), charSet));
				String ua = new String(realMatcher.group(10).getBytes(), charSet);
				if (ua.contains("Tudou") || ua.contains("tudou")) {
					isTudou = "1";
				}
				vec.add(ua);
				if (realMatcher.groupCount() > 10 && realMatcher.group(11).length() > 0) {
					// server ip
					Matcher seripv4 = Ipv4.matcher(realMatcher.group(11).toString());
					if (seripv4.find()) {
						server = seripv4.group(1);
					} else {
						String[] lastcols = realMatcher.group(11).toString().trim().split("\\s");
						server = lastcols[0];
					}
				}
				vec.add(server.toString());
				vec.add(isTudou);
			} else {
				return new ArrayList<String>();
			}
			// System.out.println("metcher " + vec.size() + " " +
			// vec.toString());
			return formatAccessLog(vec);
		} catch (Exception e) {
			e.printStackTrace();
			// System.out.println("logsplit " + vec.size() + " " +
			// vec.toString());
			return vec;
		}
		
	}
	
	@Override
	public ArrayList<String> logSplit(String str, String space) {
		ArrayList<String> vec = new ArrayList<String>();
		try {
			// str = new String(str.getBytes("8859_1"), charSet); // 编码转换
			// default RealLogPattern
			String server = "00";
			String isTudou = "0";
			Matcher pm2 = syslogExtPer.matcher(str);
			Matcher ipv4 = Ipv4.matcher(str);
			String syslogper = "";
			// 从syslog增加的2列里取hostname序号
			if (pm2.find()) {
				syslogper = pm2.group();
				server = pm2.group(2);
				// get运维hostid
				int attudou = server.indexOf('-');
				if (attudou > 0) {
					// tudo
					server = server.substring(0, attudou);
				} else {
					server = server.replaceAll("[^\\d]+", "");
				}
				if (syslogper.contains("Tudou") || syslogper.contains("tudou")) {
					isTudou = "1";
				}
				
			}
			
			// 去掉运维加的2列,从ip开始算正试日志
			if (ipv4.find()) {
				str = str.substring(ipv4.start());
			} else {
				str = str.replaceAll(syslogExtPer.toString(), "");
			}
			
			// split
			int start = 0;
			int end = 0;
			end = str.indexOf(space);
			if (end == -1) {
				return vec;
			}
			// ip
			String v = sublogString(str, start, end);
			vec.add(v);
			
			// date time
			start = end + 2;
			end = str.indexOf("\"", start);
			v = sublogString(str, start, end);
			vec.add(v);
			
			// method
			start = end + 2;
			end = str.indexOf(space, start);
			v = sublogString(str, start, end);
			vec.add(v);
			
			// uri
			start = end + 2;
			end = str.indexOf("\"", start);
			v = sublogString(str, start, end);
			vec.add(v);
			
			// arg
			start = end + 3;
			end = str.indexOf("\"", start);
			v = sublogString(str, start, end);
			vec.add(v);
			
			// req body
			start = end + 2;
			end = str.indexOf(space, start);
			v = sublogString(str, start, end);
			vec.add(v);
			
			// req code
			start = end + 1;
			end = str.indexOf(space, start);
			v = sublogString(str, start, end);
			vec.add(v);
			
			// req size
			start = end + 1;
			end = str.indexOf(space, start);
			v = sublogString(str, start, end);
			vec.add(v);
			
			// req time
			start = end + 1;
			end = str.indexOf(space, start);
			v = sublogString(str, start, end);
			vec.add(v);
			
			// req ua
			start = end + 2;
			end = str.indexOf("\"", start + 1);
			v = sublogString(str, start, end);
			if (v.contains("Tudou") || v.contains("tudou")) {
				isTudou = "1";
			}
			vec.add(v);
			
			// req serve
			start = end + 2;
			end = str.indexOf(space, start);
			v = sublogString(str, start, end);
			if (v == null || v.length() == 0) {
				vec.add(server);
			} else {
				vec.add(v);
			}
			vec.add(isTudou);
		} catch (Exception e) {
			e.printStackTrace();
			return new ArrayList<String>();
		}
		return formatAccessLog(vec);
	}
	
	@Override
	public String sublogString(String str, int start, int end) {
		String val = new String();
		if (start > str.length()) {
			start = str.length();
		}
		if (end == -1) {
			val = str.substring(start);
		} else {
			val = str.substring(start, end);
		}
		val = val.trim();
		if (val.startsWith("\"")) {
			val = val.replaceAll("^\"", "");
		}
		if (val.endsWith("\"")) {
			val = val.replaceAll("\"$", "");
		}
		return val;
		
	}
	
	@Override
	public void logExpandsToObject(ArrayList<String> itr) {
		// 其本字段to obj
		this.logToObject(itr);
		// 扩展log字段 and to obj
		this.logExpands(itr);
	}
	
	/**
	 * to object and logExpands
	 */
	public void logToObject(ArrayList<String> itr) {
		try {
			if (itr.size() >= 10) {
				// ip datetime method uri args req_body code req_length req_time
				// ua server+ kls uri_name pid
				// LOGR.info("logToObject line ok, size " + itr.size() +
				// itr.toString());
				this.setLogVer(0);
				String ip = itr.get(0).toString();
				this.setIp(ip);
				String datestr = itr.get(1).toString();
				this.setTime(Common.dateLong(datestr));
				this.setDate_time(datestr);
				this.setMethod(itr.get(2).toString());
				this.setUri(new String(itr.get(3).toString().getBytes(), charSet));
				this.setRequest_args(itr.get(4).toString());
				this.setRequest_body(itr.get(5).toString());
				this.setResponse_code(Integer.valueOf(itr.get(6).toString()));
				this.setContent_length(Long.valueOf(itr.get(7).toString()));
				// Request_time 秒转成ms
				BigDecimal dw = new BigDecimal(1000);
				dw = dw.multiply(new BigDecimal(itr.get(8).toString()));
				this.setRequest_time(dw.longValue());
				this.setUser_agent(itr.get(9).toString());
				this.setServer(itr.get(10).toString());
				this.setRely(itr.get(11).toString());
			}
			
		} catch (Exception e) {
			e.printStackTrace();
			LOGR.info("logToObject Exception ,line size " + itr.size() + " " + itr.toString());
			LOGR.info(e.toString());
		}
	}
	
	public void logExpands(ArrayList<String> itr) {
		try {
			if (itr.size() >= 10) {
				String method = new String(itr.get(2).toString().getBytes(), charSet);
				String uri = new String(itr.get(3).toString().getBytes(), charSet);
				String tmp_args = new String(itr.get(4).toString().getBytes(), charSet) + "&"
						+ new String(itr.get(5).toString().getBytes(), charSet);
				
				String[] args = tmp_args.split("&");
				Map<String, String> map = new HashMap<String, String>();
				for (String arg : args) {
					if (arg == null || arg.equals("=")) {
						continue;
					}
					String[] key_value = arg.split("=");
					String key = key_value[0];
					String value = key_value.length == 2 ? key_value[1] : "";
					map.put(key, value);
				}
				String pid = map.get("pid");
				if (pid == null) {
					pid = "";
				}
				String guid = map.get("guid");
				if (guid == null) {
					guid = "";
				}
				this.setPid(pid);
				this.setGuid(guid);
				if (this.getUser_agent().matches("Tudo")) {
					this.setRely("1");
				}
				
				HashMap regmap = null;
				String uri_name = uri;
				// 去掉开始的/,再用/分割，再取数组的第一个字段做为kls
				String[] uriarr = uri.toString().trim().replaceFirst("\\/", "").split("\\/");
				String kls = uriarr.length > 0 ? uriarr[0] : "other";
				String mats = AppsConfig.getInstance().getValue("access.log.matches").toString();
				if (Common.SpecialRegex.matcher(uri).find() || uriarr.length > 4) {
					// is default matcher
					regmap = (HashMap) Common.MatcherUri(uriRegex, uri, method.toUpperCase());
				}
				if (!mats.isEmpty()) {
					mats = mats.replaceAll(",", "|");
				}
				if (regmap == null && mats.length() > 0 && Pattern.compile(mats).matcher(uri).find()) {
					// is plus from conf access.log.matches key ,matcher
					regmap = (HashMap) Common.MatcherUri(uriRegex, uri, method.toUpperCase());
				}
				if (regmap != null && regmap.size() > 1) {
					uri_name = (String) regmap.get("uri_name");
					kls = (String) regmap.get("kls");
					String[] tmparr = uri_name.toString().trim().replaceFirst("\\/", "").split("\\/");
					if (tmparr.length > 4) {
						// 太长了截掉
						uri_name = String.format("/%s/%s/%s/%s", tmparr[0], tmparr[1], tmparr[2], tmparr[3]);
					}
				} else {
					// 按顺来处理特殊uri非法输入
					if (Common.Digit.matcher(kls).find()) {
						// 非法输入 1
						if (uri.length() > 20) {
							uri_name = uri.substring(0, 20);
						} else {
							uri_name = uriarr[0];
						}
						kls = "other";
					}
					if (kls.length() < 2) {
						// // 非法输入 2
						uri_name = uriarr.length > 0 ? uriarr[0] : uri;
						kls = "other";
					}
					String[] tmparr = uri_name.toString().trim().replaceFirst("\\/", "").split("\\/");
					if (tmparr.length > 3) {
						// 非法未知情况
						uri_name = String.format("/%s/%s/%s", tmparr[0], tmparr[1], tmparr[2]);
						kls = "other";
					}
				}
				
				this.setUri_name(uri_name);
				this.setKls(kls);
				String prov = "0000000000";
				String city = "0000000000";
				
			}
		} catch (Exception e) {
			e.printStackTrace();
			LOGR.info("logExpands Exception ,line size " + itr.size() + " " + itr.toString());
			LOGR.info(e.toString());
		}
	}
	
	@Override
	public ArrayList<String> formatAccessLog(ArrayList<String> alog) {
		try {
			for (int i = 0; i < alog.size(); i++) {
				switch (i) {
					case 0:
						// ip
						break;
					case 1:
						// date
						String date = alog.get(i).toString();
						date = date.replace("T", " ");
						date = date.replace("+08:00", "");
						alog.set(i, date);
						break;
					case 2:
						// method
						alog.set(i, alog.get(i).toString().toUpperCase());
						break;
					case 3:
						// uri spit by / replace vid uid keyword
						String uri = alog.get(i).toString();
						uri = Common.TrimUri(uri);
						alog.set(i, uri);
						break;
					case 9:
						// user_agent
						String val = alog.get(i).toString();
						if (user_agent == null) {
							val = "";
						} else {
							try {
								val = URLDecoder.decode(val, "UTF-8");
							} catch (UnsupportedEncodingException une) {
								une.printStackTrace();
							}
						}
						alog.set(i, val);
						break;
					case 10:
						// server ip
						String[] tmpips = alog.get(i).toString().trim().split("\\s");
						String strip = tmpips[0];
						if (strip.length() == 0) {
							strip = "00";
						}
						alog.set(i, strip);
						break;
					default:
						alog.set(i, alog.get(i).toString().trim());
				}
			}
			return alog;
			
		} catch (Exception e) {
			LOGR.info("formatAccessLog ", e.toString());
			return alog;
		}
	}
	
	@Override
	public String repr() {
		String report = String.format("%s", this.valToString("#"));
		return report;
	}
	
	@Override
	public String toString() {
		return String.format("Accesslog [%s]", this.valToString("#"));
	}
	
	/**
	 * is base to string
	 * 
	 * @param delm
	 * @return base field
	 */
	@Override
	public String valToString(String delm) {
		StringBuffer val = new StringBuffer();
		val.append(getIp()).append(delm);
		val.append(getDate_time()).append(delm);
		val.append(getMethod()).append(delm);
		val.append(getUri_name()).append(delm);
		val.append(getResponse_code()).append(delm);
		val.append(getContent_length()).append(delm);
		val.append(getRequest_time()).append(delm);
		val.append(getServer()).append(delm);
		val.append(getRely());
		return val.toString();
	}
	
	public String toJsonString() {
		return JacksonUtils.objectToJson(this);
	}
	
	public String outKey() {
		// 请勿随意改动
		// 转成long分，抛弃秒值set00
		// ukey=daytime#rely#server
		// 20120613#10:01:00#0#/servernum
		StringBuffer sb = new StringBuffer();
		String SEPARATOR = "#";
		if (getLogVer() == 1) {
			// ver=1
			sb.append(getLogVer()).append(SEPARATOR).append(Common.formatDataTimeStr1(getDate_time()))
					.append(SEPARATOR).append(getRely()).append(SEPARATOR).append(getServer()).append(SEPARATOR)
					.append(getUri_name());
		} else {
			// ver=0
			sb.append(getLogVer()).append(SEPARATOR).append(Common.formatDataTimeStr1(getDate_time()))
					.append(SEPARATOR).append(getRely()).append(SEPARATOR).append(getServer());
		}
		return sb.toString();
	}

	
	public int getLogVer() {
		return logVer;
	}
	
	public void setLogVer(int logVer) {
		this.logVer = logVer;
	}
	
	/**
	 * 获取访问者IP地址
	 * 
	 * @return
	 */
	public String getIp() {
		return ip;
	}
	
	/**
	 * 访问者的IP地址
	 * 
	 * @param ip
	 */
	public void setIp(String ip) {
		this.ip = ip;
	}
	
	public long getTime() {
		return time;
	}
	
	public void setTime(long time) {
		this.time = time;
	}
	
	public String getDate_time() {
		try {
			if (this.date_time != null && this.date_time.length() > 20) {
				this.date_time = this.date_time.replace("T", " ");
				this.date_time = date_time.replace("+08:00", "");
			}
		} catch (Exception e) {
			// e.printStackTrace();
		}
		return this.date_time;
	}
	
	public void setDate_time(String date) {
		if (date != null && date.length() > 20) {
			date = date.replace("T", " ");
			date = date.replace("+08:00", "");
		}
		this.date_time = date;
	}
	
	public String getMethod() {
		return method;
	}
	
	public void setMethod(String method) {
		this.method = method;
	}
	
	public String getUri() {
		return uri;
	}
	
	public void setUri(String uri) {
		this.uri = uri;
	}
	
	public String getRequest_args() {
		return request_args;
	}
	
	public void setRequest_args(String request_args) {
		this.request_args = request_args;
	}
	
	public String getRequest_body() {
		return request_body;
	}
	
	public void setRequest_body(String request_body) {
		this.request_body = request_body;
	}
	
	public int getResponse_code() {
		return response_code;
	}
	
	public void setResponse_code(int response_code) {
		this.response_code = response_code;
	}
	
	public long getContent_length() {
		return content_length;
	}
	
	public void setContent_length(long content_length) {
		this.content_length = content_length;
	}
	
	public long getRequest_time() {
		return request_time;
	}
	
	public void setRequest_time(long request_time) {
		this.request_time = request_time;
	}
	
	public String getUser_agent() {
		return user_agent;
	}
	
	public void setUser_agent(String user_agent) {
		this.user_agent = user_agent;
	}
	
	public String getPid() {
		return pid;
	}
	
	public void setPid(String pid) {
		this.pid = pid;
	}
	
	public String getKls() {
		return kls;
	}
	
	public void setKls(String kls) {
		this.kls = kls;
	}
	
	public String getUri_name() {
		return uri_name;
	}
	
	public void setUri_name(String uri_name) {
		this.uri_name = uri_name;
	}
	
	public String getProv() {
		return prov;
	}
	
	public void setProv(String prov) {
		this.prov = prov;
	}
	
	public String getCity() {
		return city;
	}
	
	public void setCity(String city) {
		this.city = city;
	}
	
	public String getServer() {
		return server;
	}
	
	public void setServer(String server) {
		this.server = server;
	}
	
	public String getGuid() {
		return guid;
	}
	
	public void setGuid(String guid) {
		this.guid = guid;
	}
	
	public String getRely() {
		return rely;
	}
	
	public void setRely(String rely) {
		this.rely = rely;
	}
	
	/**
	 * 获取日志日期,格式为yyyyMMdd
	 * 
	 * @return
	 */
	public String getDateStr() {
		return yyyyMMdd_sdf.format(new Date(time));
	}
	
	private static final SimpleDateFormat yyyyMMdd_sdf = new SimpleDateFormat("yyyyMMdd");
	
	/**
	 * 获取访问者IP地址,以int的形式
	 * 
	 * @return
	 */
	public int getIpInt() {
		int iip = ipToInt(this.ip.split("\\."));
		return iip;
	}
	
	/**
	 * 获取日志时间，{@link java.util.Calendar}
	 * 
	 * @return
	 */
	public Calendar getTimeCalendar() {
		Calendar c = Calendar.getInstance();
		c.setTimeInMillis(time);
		return c;
	}
	
	/**
	 * 获取日志时间，{@link java.util.Date}
	 * 
	 * @return
	 */
	public Date getTimeDate() {
		return new Date(time);
	}
	
	/**
	 * 获取日志时间，格式化为Long的字符串格式
	 * 
	 * @return
	 */
	public String getTimeStr() {
		return String.valueOf(time);
	}
	
	private int ipToInt(String[] strs) {
		if (strs.length != 4) {
			return 0;
		}
		int ipInt = (parseInt(strs[0]) << 24) | (parseInt(strs[1]) << 16) | (parseInt(strs[2]) << 8)
				| (parseInt(strs[3]));
		return ipInt;
	}
	
	/**
	 * 所记录的IP是否有代理信息
	 * 
	 * @return
	 */
	public boolean isProxyIp() {
		return ip.contains(",");
	}
	
	private static final int parseInt(String str) {
		if (str == null || str.length() == 0) {
			return 0;
		} else {
			try {
				return Integer.parseInt(str);
			} catch (Exception e) {
				return 0;
			}
		}
	}
	
	@Override
	public Logger getLogger() {
		return LOGR;
	}
}
