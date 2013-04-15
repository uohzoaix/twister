package simple;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

public class FileTest {
	// 主方法
	public static void main(String[] args) {
		listFile("/tmp/a.txt");
		fileCopy("/tmp/words.txt", "/tmp/b.txt");
		fileRead("/tmp/words.txt");

	}

	public static void listFile(String filename) {
		File file = new File(filename);
		if (!file.exists()) {
			try {
				file.createNewFile();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		File dir = new File(file.getParent());
		if (dir.isDirectory()) {
			String[] fs = dir.list();
			for (String fileName : fs) {
				File f = new File(dir.getPath() + File.separator + fileName);
				if (f.isFile()) {
					System.out.println("is file " + f.toString());
				} else if (f.isDirectory()) {
					System.out.println("is dir " + f.toString());
				}
			}
		}
	}

	public static void fileCopy(String in, String out) {
		try {
			FileInputStream fi=new FileInputStream(in);
			FileOutputStream fo=new FileOutputStream(out);
			byte[] buff = new byte[256];
			int len = 0;
			while ((len = fi.read(buff)) > 0) {
				fo.write(buff, 0, len);
			}
			fi.close();
			fo.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}

	public static void fileRead(String in) {
		try {
			InputStream fi = new FileInputStream(in);
			InputStreamReader isr = new InputStreamReader(fi);
			BufferedReader br = new BufferedReader(isr);
			String str = null;
			while ((str = br.readLine()) != null) {
				System.out.println(str);
			}
			fi.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
