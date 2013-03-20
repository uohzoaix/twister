package com.twister.simple;

import java.io.RandomAccessFile;
import com.twister.nio.log.AccessLog;
import com.twister.nio.log.AccessLogAnalysis;
import com.twister.utils.CharsetUtil;
import com.twister.utils.JacksonUtils;

public class TestLog {
	private static String vv = "Mar  5 10:59:59 a02 112.117.200.169 \"2013-03-03T00:00:14+08:00\" GET \"/home\" \"pid=10ec7521b331887d&t=1362240233&e=md5&s=0f0ef2b2fe756bf3aa8f71ba97734557&guid=a299fee374d507d34968dc65ba5cf558\" \"-\" 200 3326 0.012 \"Tudou;3.0;Android;2.3.4;LT18i\"";
	private static String vv2 = "Mar  5 10:59:59 a02-api-3g-b28-tudou 112.117.200.169 \"2013-03-03T00:00:14+08:00\" GET \"/videos/XMjI3MzIwMzU2/download\" \"pid=10ec7521b331887d&t=1362240233&e=md5&s=0f0ef2b2fe756bf3aa8f71ba97734557&guid=a299fee374d507d34968dc65ba5cf558\" \"-\" 200 3326 0.012 \"Mozilla/5.0 (X11; Linux x86_64; rv:10.0.5) Gecko/20120606 Firefox/10.0.5\" 10.103.13.18";
    public int abc=1;
	 
   
	
	public int getAbc() {
		return abc;
	}
	public void setAbc(int abc) {
		this.abc = abc;
	}
	
	public static void main(String[] args) {
		try {
			testBytext(); 
			 
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
	}
	public void hello(String str){
		System.out.println("method hello() !!! "+str);
	}
 
	
	public static void testBytext() throws Exception {		
		String logfile = "src/main/resources/accessLog.txt";
		RandomAccessFile file = new RandomAccessFile(logfile, "r");
		long filePointer = 0;
		boolean issend = true;
		 AccessLog alog2 = new AccessLog(vv);
		System.out.println(alog2);
		AccessLogAnalysis alys = new  AccessLogAnalysis(alog2.outKey(),alog2.getResponse_code(),alog2.getContent_length(),alog2.getRequest_time());
		System.out.println(alys.objectToJson());
		// String
		// jstr="{\"key\":\"20130303#00:00:00#1#/home\",\"day\":\"20130303\",\"cnt_pv\":1,\"cnt_bytes\":3326,\"cnt_time\":12,\"avg_time\":12.0,\"code\":200,\"cnt_error\":0,\"a\":1,\"b\":0,\"c\":0,\"d\":0,\"e\":0}";
		 System.out.println(JacksonUtils.jsonToObject(alys.objectToJson(),AccessLogAnalysis.class));
		issend = true;
		 
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
					String packet = new String(line.getBytes("8859_1"), CharsetUtil.UTF_8); // 编码转换
					i++;
					//if (i == 100)
					//	return;
					AccessLog alog = new AccessLog(packet);
					System.out.println(i+ " "+alog.toString());
					
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
