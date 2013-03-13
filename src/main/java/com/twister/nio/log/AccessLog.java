package com.twister.nio.log;

import java.io.RandomAccessFile;
import java.util.ArrayList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AccessLog extends AbstractAccessLog {
	
	private static final long serialVersionUID = 4224713360551345643L;
	
	private static Logger LOGR = LoggerFactory.getLogger(AccessLog.class);
	
	private static String vv = "Mar  5 10:59:59 a01 112.117.200.169 \"2013-03-03T00:00:14+08:00\" GET \"/home\" \"pid=10ec7521b331887d&t=1362240233&e=md5&s=0f0ef2b2fe756bf3aa8f71ba97734557&guid=a299fee374d507d34968dc65ba5cf558\" \"-\" 200 3326 0.012 \"Tudou;3.0;Android;2.3.4;LT18i\"";
	private static String vv2 = "Mar  5 10:59:59 a01-api-3g-b28-tudou 112.117.200.169 \"2013-03-03T00:00:14+08:00\" GET \"/videos/XMjI3MzIwMzU2/download\" \"pid=10ec7521b331887d&t=1362240233&e=md5&s=0f0ef2b2fe756bf3aa8f71ba97734557&guid=a299fee374d507d34968dc65ba5cf558\" \"-\" 200 3326 0.012 \"Mozilla/5.0 (X11; Linux x86_64; rv:10.0.5) Gecko/20120606 Firefox/10.0.5\" 10.103.13.18";
	private ArrayList uriRegex = null;
	
	public AccessLog() {
	}
	
	public AccessLog(String line) {
		this.initSettings();
		ArrayList<String> alog = parseLog(line);
		this.logExpandsToObject(alog);
	}
	
	public static void main(String[] args) {
		try {
			testBytext();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	@Override
	public String toString() {
		return super.toString();
	}
	
	@Override
	public Logger getLogger() {
		return LOGR;
	}
	
	public static void testBytext() throws Exception {
		
		String logfile = "src/main/resources/accessLog.txt";
		RandomAccessFile file = new RandomAccessFile(logfile, "r");
		long filePointer = 0;
		boolean issend = true;	 
		//AccessLog alog2 = new AccessLog(vv);		 
		 
		//String jstr="{\"key\":\"1362240000000|GET|/home|200|1|01|0000000000\",\"day\":\"20130303\",\"cnt_pv\":1,\"cnt_bytes\":3326,\"cnt_time\":12,\"avg_time\":12.0,\"cnt_error\":0,\"a\":1,\"b\":0,\"c\":0,\"d\":0,\"e\":0}";
		//System.out.println(JacksonUtils.jsonToObject(alys.objectToJson(),AccessLogAnalysis.class));
		issend = false;
		while (issend) {
			long fileLength = logfile.length();
			if (fileLength < filePointer) {
				file = new RandomAccessFile(logfile, "r");
				filePointer = 0;
			}
			if (fileLength > filePointer) {
				file.seek(filePointer);
				String line = null;
				
				int i = 0;
				while ((line = file.readLine()) != null) {
					String packet = new String(line.getBytes("8859_1"), charSet); // 编码转换
					i++;
					if (i == 100)
						return;
					AccessLog alog = new AccessLog(packet); 
					System.out.println(alog.repr());
					
				}
				filePointer = file.getFilePointer();
				if (line == null) {
					issend = false;
					break;
				}
			}
		}
		file.close();
		
	}
}
