package simple;

/**
*use python
def ip2int(ipStr):
    ip = str(ipStr).split('.')
    ipInt = (long(ip[0]) << 24) + (long(ip[1]) << 16) + (long(ip[2]) << 8) + long(ip[3])
    return long(ipInt)    
 
def int2ip(ip):
    ip = long(ip)   
    ipStr = "{}.{}.{}.{}".format((ip >> 24) & 0xFF , (ip >> 16) & 0xFF, (ip >> 16) & 0xFF , ip & 0xFF)   
    return ipStr    
 * @author guoqing
 *
 */
public class IpUtil {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// 2130706433
		System.out.println(Integer.MAX_VALUE);
		String ip = "202.106.0.20";
		long intip = ip2int(ip);
		String strip = ip2str(intip);// 3395944468
		System.out.println(ip + " " + intip + " " + strip);

	}

	/**
	 * (ip[0]<<24)+(ip[1]<<16)+(ip[2]<<8)+ip[3];
	 * (ip[0]<<24)|(ip[1]<<16)|(ip[2]<<8)|ip[3];
	 * //ip1*256*256*256+ip2*256*256+ip3*256+ip4
	 * @param ip
	 * @return (ip[0]<<24)+(ip[1]<<16)+(ip[2]<<8)+ip[3];
	 */

	public static long ip2int(String ip) {
		String[] tmp = ip.split("\\.");
		long ipint = (long) ((Long.valueOf(tmp[0]) << 24) | (Long.valueOf(tmp[1]) << 16) | (Long.valueOf(tmp[2]) << 8) | (Long.valueOf(tmp[3])));
		return ipint;
	}

	/**
	 * @param ip
	 * @return	 ((ip >> 24) & 0xFF) + "." + ((ip >> 16) & 0xFF) + "." + ((ip >> 16) & 0xFF) + "." + (ip & 0xFF)
	 */

	public static String ip2str(long ip) {
		StringBuffer sb = new StringBuffer();
		sb.append((ip >> 24) & 0xFF).append(".").append((ip >> 16) & 0xFF).append(".").append((ip >> 16) & 0xFF).append(".").append(ip & 0xFF);
		return sb.toString();
	}

}
