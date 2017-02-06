package org.evaluation.utils;

/**
 * Created by heytitle on 12/4/16.
 */
public class Log {

	private static boolean is_debugging = Boolean.parseBoolean(System.getenv("JAVA_DEBUG"));

	public static void debug(String msg){
		if(is_debugging){
			StackTraceElement st = new Exception().getStackTrace()[1];
			String caller = st.getClassName();
			String mn = st.getMethodName();
			String str = String.format("[%s:%s] %s", caller, mn, msg);
			System.out.println(str);
		}
	}
}
