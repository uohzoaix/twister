package com.twister.simple;

import java.io.RandomAccessFile;
import com.twister.nio.log.AccessLog;
import com.twister.utils.CharsetUtil;
 
 

public class TestLog {
	private static String vv = "Mar  5 10:59:59 a01 112.117.200.169 \"2013-03-03T00:00:14+08:00\" GET \"/home\" \"pid=10ec7521b331887d&t=1362240233&e=md5&s=0f0ef2b2fe756bf3aa8f71ba97734557&guid=a299fee374d507d34968dc65ba5cf558\" \"-\" 200 3326 0.012 \"Tudou;3.0;Android;2.3.4;LT18i\"";
	private static String vv2 = "Mar  5 10:59:59 a01-api-3g-b28-tudou 112.117.200.169 \"2013-03-03T00:00:14+08:00\" GET \"/videos/XMjI3MzIwMzU2/download\" \"pid=10ec7521b331887d&t=1362240233&e=md5&s=0f0ef2b2fe756bf3aa8f71ba97734557&guid=a299fee374d507d34968dc65ba5cf558\" \"-\" 200 3326 0.012 \"Mozilla/5.0 (X11; Linux x86_64; rv:10.0.5) Gecko/20120606 Firefox/10.0.5\" 10.103.13.18";
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
		// String
		// jstr="{\"key\":\"1362240000000|GET|/home|200|1|01|0000000000\",\"day\":\"20130303\",\"cnt_pv\":1,\"cnt_bytes\":3326,\"cnt_time\":12,\"avg_time\":12.0,\"cnt_error\":0,\"a\":1,\"b\":0,\"c\":0,\"d\":0,\"e\":0}";
		// System.out.println(JacksonUtils.jsonToObject(alys.objectToJson(),AccessLogAnalysis.class));
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
					String packet = new String(line.getBytes("8859_1"), CharsetUtil.UTF_8); // 编码转换
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
