package com.twister.io.input;

import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.twister.utils.AppsConfig;
import com.twister.utils.Common;

public abstract class AbstractAccessLog implements IAccessLog {
	public static Logger LOGR = LoggerFactory
			.getLogger(AbstractAccessLog.class);

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
	public ArrayList<Map<String, Serializable>> uriRegex = null;

	public AbstractAccessLog() {
	}

	public AbstractAccessLog(String line) {
		ArrayList<String> alog = parseLog(line);
		this.logToObject(alog);
	}

	public void init(AbstractAccessLog alog) {
		uriRegex = Common.getUriRegexConf(Common.UriRegexFile);
	}

	public boolean checkAccessLog(String line) {
		return true;
	}

	public ArrayList<String> parseLog(String line) {
		if (this.checkAccessLog(line)) {
			ArrayList<String> itr = this.logMatcher(line);
			return itr;
		} else {
			return new ArrayList<String>();
		}
	}

	public ArrayList<String> logMatcher(String line) {
		ArrayList<String> vec = new ArrayList<String>();
		String srcline = new String(line);
		try {
			// default RealLogPattern
			line = new String(line.getBytes("8859_1"), charSet); // 编码转换
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
				vec.add(realMatcher.group(1).toString());
				vec.add(realMatcher.group(2).toString());
				vec.add(realMatcher.group(3).toString());
				vec.add(realMatcher.group(4).toString());
				vec.add(realMatcher.group(5).toString());
				vec.add(realMatcher.group(6).toString());
				vec.add(realMatcher.group(7).toString());
				vec.add(realMatcher.group(8).toString());
				vec.add(realMatcher.group(9).toString());
				vec.add(realMatcher.group(10).toString());
				if (realMatcher.groupCount() > 10
						&& realMatcher.group(11).length() > 0) {
					server = realMatcher.group(11);
				}
				vec.add(server.toString());
			} else {
				return new ArrayList<String>();
			}
			System.out.println("metcher " + vec.size() + " " + vec.toString());
		} catch (Exception e) {
			e.printStackTrace();
			return logSplit(srcline, SPACE);
		}
		return formatAccessLog(vec);
	}

	public ArrayList<String> logSplit(String str, String regex) {
		ArrayList<String> vec = new ArrayList<String>();
		try {
			str = new String(str.getBytes("8859_1"), charSet); // 编码转换
			// default RealLogPattern
			Matcher pm2 = syslogExtPer.matcher(str);
			String server = "00";
			boolean aserflag = false;
			if (pm2.find()) {
				server = pm2.group(2);
				// get运维hostid
				server = server.replaceAll("[^\\d]+", "");
				// 去掉运维加的2列
				str = str.replaceAll(syslogExtPer.toString(), "");
				str = str.trim();
				aserflag = true;
			}
			int len = str.length();
			int start = 0;
			int end = 0;
			boolean rd = true;
			end = str.indexOf(regex);
			if (end == -1) {
				rd = false;
				return null;
			}
			StringBuffer buf = new StringBuffer();
			while (rd) {
				String val = str.substring(start, end);
				buf.append(val);
				if (val.startsWith("\"") && !val.endsWith("\"")) {
					start = end + 1;
					end = str.indexOf("\"", start);
					String val2 = str.substring(start, end);
					buf.append(val2);
				}
				start = end + 1;
				end = str.indexOf(regex, start);
				if (end == -1) {
					rd = false;
					String val2 = str.substring(start, len);
					buf.append(val2);
				}
				String field = buf.toString();
				if (field.startsWith("\"")) {
					field = field.replaceAll("^\"", "");
				}
				if (field.endsWith("\"")) {
					field = field.replaceAll("\"$", "");
				}
				vec.add(field);
				buf = null;
				buf = new StringBuffer();
			}
			if (aserflag && vec.size() < 11) {
				vec.add(server);
			}
			System.out.println("split " + vec.size() + " " + vec.toString());
		} catch (Exception e) {
			e.printStackTrace();
			return new ArrayList<String>();
		}
		return formatAccessLog(vec);
	}

	public void logToObject(ArrayList itr) {
		try {
			if (itr.size() >= 10) {
				// ip datetime method uri args req_body code req_length req_time
				// ua server+ kls uri_name pid
				LOGR.info("logToObject line ok, size "  + itr.size() + itr.toString());
				this.setLogVersion('0');
				String ip = itr.get(0).toString();
				this.setIp(ip);
				String datestr = itr.get(1).toString();
				this.setTime(Common.dateLong(datestr));
				this.setDate_time(datestr);
				this.setMethod(itr.get(2).toString());
				this.setUri(itr.get(3).toString());
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
				// 扩展log字段
				this.logExpands(itr);
			}

		} catch (Exception e) {
			e.printStackTrace();
			LOGR.info("logToObject Exception ,line size " + itr.size() + " "
					+ itr.toString());
			LOGR.info(e.toString());
		}
	}

	public void logExpands(ArrayList itr) {
		try {
			if (itr.size() >= 10) {
				String method = itr.get(2).toString();
				String uri = itr.get(3).toString();
				String tmp_args = itr.get(4).toString() + "&"
						+ itr.get(5).toString();

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
				String pid = (String) map.get("pid");
				if (pid == null) {
					pid = "";
				}
				String guid = (String) map.get("guid");
				if (guid == null) {
					guid = "";
				}
				this.setPid(pid);
				this.setGuid(guid);
				HashMap regmap = null;
				String[] uriarr = uri.toString().split("\\/");
				String kls = uriarr.length > 0 ? uriarr[0] : uri;
				String uri_name = uri;
				String rely = "0";
				if (this.getUser_agent().matches("Tudo")) {
					rely = "1";
				}
				String mats = AppsConfig.getInstance()
						.getValue("access.log.matches").toString();
				if (uri.matches("videos|search|shows|user|channels")) {
					// is default matcher
					regmap = (HashMap) Common.MatcherUri(uriRegex, uri,
							method.toUpperCase()); // matcher uri
				}

				if (regmap == null && !mats.isEmpty()) {
					// is from conf access.log.matches key ,matcher
					mats = mats.replaceAll(",", "|");
					if (Pattern.compile(mats).matcher(uri).find()) {
						regmap = (HashMap) Common.MatcherUri(uriRegex, uri,
								method.toUpperCase()); // matcher uri
					}
				}
				if (regmap != null && regmap.size() > 1) {
					uri_name = (String) regmap.get("uri_name");
					kls = (String) regmap.get("kls");
				}
			 
				this.setUri_name(uri_name);
				this.setKls(kls);
				this.setRely(rely);
				String prov = "0000000000";
				String city = "0000000000";

			}
		} catch (Exception e) {
			e.printStackTrace();
			LOGR.info("logExpands Exception ,line size " + itr.size() + " "
					+ itr.toString());
			LOGR.info(e.toString());
		}
	}
 

	public ArrayList formatAccessLog(ArrayList alog) {
		// [112.117.200.169, 2013-03-03T00:00:14+08:00, GET, /home,
		// pid=10ec7521b331887d&t=1362240233&e=md5&s=0f0ef2b2fe756bf3aa8f71ba97734557&guid=a299fee374d507d34968dc65ba5cf558,
		// -, 200, 3326, 0.012, Tudou;3.0; Android; 2.3.4; LT18i, 10.103.13.18]
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
				uri =Common.TrimUri(uri);
				alog.set(i, uri);
				break;
			case 9:
				// user_agent
				String val = alog.get(i).toString();
				System.out.println(val);
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
				//server ip
				String strip=alog.get(i).toString().trim();
				if (strip.length() == 0) {
					strip = "00";
				}
				alog.set(i,strip);
				break;
			default:
				alog.set(i, alog.get(i).toString().trim());
			}
		}	
		return alog;
	}

	public String repr() {
		return "{" + "logVersion=" + getLogVersion() + ", time=" + getTime()
				+ ", ip=" + getIp() + ", date_time=" + getDate_time()
				+ ", method=" + getMethod() + ", uri=" + getUri()
				+ ", request_args=" + getRequest_args() + ", request_body="
				+ getRequest_body() + ", response_code=" + getResponse_code()
				+ ", content_length=" + getContent_length() + ", request_time="
				+ getRequest_time() + ", user_agent=" + getUser_agent()
				+ ", ser=" + getServer() + ", kls=" + getKls() + ", uri_name="
				+ getUri_name() + ", pid=" + getPid() + " }";
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
