package deltad.core;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;

public class RegexUtil {
	
	private static final Logger logger = Logger.getLogger(RegexUtil.class.getName());

	public static boolean likeMatch(String source, String like_pattern) {
		final String WILDCARD_N = "[a-zA-Z_0-9]*";
		final String WILDCARD_ONE = "[a-zA-Z_0-9]";
//		final String ANYONE = "([\\s\\S])";
//		final String ANY = "([\\s\\S]*)";
		String pattern = like_pattern.replaceAll("_", WILDCARD_ONE);
		pattern = pattern.replaceAll("%", WILDCARD_N);
		
		//pattern = pattern.startsWith("%") || pattern.startsWith("_") ? pattern : "^" + pattern;
		return regexMatch(source, pattern);
	}

	public static boolean regexMatch(String source, String pattern) {
		//System.out.println(source + ":" + pattern);
		Matcher matchr = Pattern.compile(pattern).matcher(source);
		while (matchr.find()) {
			String str = matchr.group();
			logger.debug("from " + matchr.start() + " to " + matchr.end() + " matches substring:" + str);
			return true;
		}
		return false;
	}
	
	public static boolean sqlLikeMatch(String source, String pattern) {
		return likeMatch(source, "^" + pattern + "$");
	}
	
	public static boolean beginWith(String source, String pattern) {
		//String ptn = pattern.startsWith("^") ? pattern : "^" + pattern;
		return likeMatch(source, "^" + pattern);
	}
	
	public static boolean endWith(String source, String pattern) {
		//String ptn = pattern.endsWith("$") ? pattern : pattern + "$";
		return likeMatch(source, pattern + "$");
	}
	
	public static void parserRac(String url) {
		Matcher matchr = Pattern.compile("\\(HOST = [0-9]*.[0-9]*.[0-9]*.[0-9]*\\)\\(PORT = [0-9]*\\)").matcher(url);
		while (matchr.find()) {
			String str = matchr.group();
			System.out.println("from " + matchr.start() + " to " + matchr.end() + " matches substring:" + str);
			//Matcher innerMatchr = Pattern.compile("[0-9]*.[0-9]*.[0-9]*.[0-9]*").matcher(str);
			str = str.replaceAll("\\(HOST = ", "");
			str = str.replaceAll("\\)\\(PORT = ", ":");
			str = str.replaceAll("\\)", "");
			System.out.println(str);			
		}
		
	}

	public static void main(String[] args) throws Exception {
		boolean matched = likeMatch("aaaa中国人abc", "%中国人abc%");
		System.out.println(matched);
		
		//regexMatch("localhost:1433;databaseName=bitest", "");
		//System.out.println(beginWith("abcdef", "ab[a-z]"));
		//System.out.println(endWith("abcdef", "[a-z]f"));
		//System.out.println(beginWith("MYTEST", "%TEST%"));
//		String connection = "(DESCRIPTION=(LOAD_BALANCE=yes)(ADDRESS = (PROTOCOL = TCP)(HOST = 20.10.1.196)(PORT = 1521))(ADDRESS = (PROTOCOL = TCP)(HOST = 20.10.1.195)(PORT = 1521))(CONNECT_DATA=(SERVER = DEDICATED)(service_name=racdb)))";
//		parserRac(connection);
	}

}
