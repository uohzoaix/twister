package com.twister.io.input;

import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.twister.utils.AppsConfig;
import com.twister.utils.Common;

public abstract class AbstractAccessLog implements IAccessLog {
	public static Logger LOGR = LoggerFactory.getLogger(AbstractAccessLog.class);
	
	// fields
	public char logVersion = '0';
	public String ip = "";
	public long time = 0L;
	public String date_time = "";
	public String method = "";
	public String uri = "";
	public String request_args = "";
	public String request_body = "";
	public int response_code = 0;
	public long content_length = 0l; // 总流量kb
	public long request_time = 0l; // 响应ms
	public String user_agent = "";
	public String server = "00";
	// ext base args pid, match uri_name klsname
	public String pid = "";
	public String kls = "other";
	public String uri_name = "other";
	public String prov = "0000000000";
	public String city = "0000000000";
	public String guid = "";
	public String rely = "0";
	public ArrayList<Map<String, Serializable>> uriRegex = new ArrayList<Map<String, Serializable>>();
	
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
		return true;
	}
	
	@Override
	public ArrayList<String> parseLog(String line) {
		if (this.checkAccessLog(line)) {
			ArrayList<String> itr = this.logMatcher(line);
			return itr;
		} else {
			return new ArrayList<String>();
		}
	}
	
	@Override
	public ArrayList<String> logMatcher(String line) {
		ArrayList<String> vec = new ArrayList<String>();
		String srcline = line;
		try {
			// default RealLogPattern
			Matcher pm2 = syslogExtPer.matcher(line);
			String server = "00";
			boolean aserflag = false;
			if (pm2.find()) {
				server = pm2.group(2);
				// get运维hostid
				server = server.replaceAll("[^\\d]+", "");
				// 去掉运维加的2列
				line = line.replaceAll(syslogExtPer.toString(), "");
				line = line.trim();
				aserflag = true;
			}
			
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
				vec.add(new String(realMatcher.group(10).getBytes(), charSet));
				if (realMatcher.groupCount() > 10 && realMatcher.group(11).length() > 0) {
					String[] lastcols = realMatcher.group(11).toString().trim().split("\\s");
					server = lastcols[0];
				}
				vec.add(server.toString());
			} else {
				return new ArrayList<String>();
			}
			// System.out.println("metcher " + vec.size() + " " +
			// vec.toString());
			return formatAccessLog(vec);
		} catch (Exception e) {
			e.printStackTrace();
			vec = logSplit(srcline, SPACE);
			return vec;
		}
		
	}
	
	@Override
	public ArrayList<String> logSplit(String str, String space) {
		ArrayList<String> vec = new ArrayList<String>();
		try {
			// str = new String(str.getBytes("8859_1"), charSet); // 编码转换
			// default RealLogPattern
			Matcher pm2 = syslogExtPer.matcher(str);
			String server = "00";
			if (pm2.find()) {
				server = pm2.group(2);
				// get运维hostid
				server = server.replaceAll("[^\\d]+", "");
				// 去掉运维加的2列
				str = str.replaceAll(syslogExtPer.toString(), "");
				str = str.trim();
			}
			int len = str.length();
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
	public void logExpandsToObject(ArrayList itr) {
		// 其本字段to obj
		this.logToObject(itr);
		// 扩展log字段 and to obj
		this.logExpands(itr);
	}
	
	/**
	 * to object and logExpands
	 */
	public void logToObject(ArrayList itr) {
		try {
			if (itr.size() >= 10) {
				// ip datetime method uri args req_body code req_length req_time
				// ua server+ kls uri_name pid
				// LOGR.info("logToObject line ok, size " + itr.size() +
				// itr.toString());
				this.setLogVersion('0');
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
			}
			
		} catch (Exception e) {
			e.printStackTrace();
			LOGR.info("logToObject Exception ,line size " + itr.size() + " " + itr.toString());
			LOGR.info(e.toString());
		}
	}
	
	public void logExpands(ArrayList itr) {
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
				HashMap regmap = null;
				String uri_name = uri;
				// 去掉开始的/,再用/分割，再取数组的第一个字段做为kls
				String[] uriarr = uri.toString().trim().replaceFirst("\\/", "").split("\\/");
				String kls = uriarr.length > 0 ? uriarr[0] : uri;
				String rely = "0";
				if (this.getUser_agent().matches("Tudo")) {
					rely = "1";
				}
				String mats = AppsConfig.getInstance().getValue("access.log.matches").toString();
				if (Pattern.compile("videos|search|shows|user|channels").matcher(uri).find()) {
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
				}
				kls = kls.toLowerCase();
				this.setUri_name(uri_name);
				this.setKls(kls);
				this.setRely(rely);
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
	public ArrayList formatAccessLog(ArrayList alog) {
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
		String report = String.format("%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s", getIp(), getDate_time(), getMethod(),
				getUri_name(), getResponse_code(), getContent_length(), getRequest_time(), getServer());
		return report;
	}
	
	public char getLogVersion() {
		return logVersion;
	}
	
	public void setLogVersion(char logVersion) {
		this.logVersion = logVersion;
	}
	
	public String getIp() {
		return ip;
	}
	
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
		return date_time;
	}
	
	public void setDate_time(String date_time) {
		this.date_time = date_time;
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
	
}
