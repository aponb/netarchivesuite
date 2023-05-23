package dk.netarkivet.onbtools.browsertrix;

import java.nio.charset.Charset;
import java.security.MessageDigest;

public class Sha1Util {

    public static String SHA1(String message) throws Exception {
        byte[] b = message.getBytes(Charset.forName("UTF-8"));
        MessageDigest md;
        md = MessageDigest.getInstance("SHA-1");
        b = md.digest(b);
            
        return byteArrayToHexString(b);
    }

    public static String byteArrayToHexString(byte[] b) {
        String result = "";
        for (byte aB : b) {
            result += Integer.toString((aB & 0xff) + 0x100, 16).substring(1);
        }
        return result;
    }
    
	public static byte[] sha1Digest(byte[] content) throws Exception {
		MessageDigest digest = MessageDigest.getInstance("SHA1");
		digest.reset();
		return digest.digest(content);
	}    
}
    