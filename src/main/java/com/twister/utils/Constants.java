package com.twister.utils;

public class Constants {
	public final static boolean isdebug = false;
	public final static int bufferSize = 1024;
	// maxFrameLength：解码的帧的最大长度
	// stripDelimiter：解码时是否去掉分隔符
	// failFast：为true，当frame长度超过maxFrameLength时立即报TooLongFrameException异常，为false，读取完整个帧再报异常

	public final static int MaxFrameLength = 1500;
	public final static boolean FailFast = false;
	public final static boolean StripDelimiter = true;
	public static final long FREQUENCY = 1L;
	public final static long OutPutTime = 5 * 60 * 1000;
	//nginxAccess 
	public static String nginxAccess = "/opt/logs/nginx/access/log";
	// mongo table
	public static String SpoutTable = "twisterServer";
	public static String ApiStatisTable = "interface";
}
