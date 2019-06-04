package com.xiaoji.duan.aag.utils;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map.Entry;

import io.netty.handler.codec.http.cookie.Cookie;
import io.netty.handler.codec.http.cookie.ServerCookieDecoder;
import io.netty.handler.codec.http.cookie.ServerCookieEncoder;
import io.vertx.core.MultiMap;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.spi.CookieStore;

public class Utils {

	public static void printHeaders(MultiMap headers) {
		for (Entry<String, String> header : headers.entries()) {
			System.out.println(header.getKey() + " : " + header.getValue());
		}
	}
	
	public static String stringHeaders(MultiMap headers) {
		StringBuffer stringheaders = new StringBuffer();
		
		for (Entry<String, String> header : headers.entries()) {
			stringheaders.append(header.getKey());
			stringheaders.append("=");
			stringheaders.append(header.getValue());
			stringheaders.append("; ");
		}
		
		return stringheaders.toString();
	}
	
	public static String getFormattedTime(long timestamp) {
		SimpleDateFormat myFmt = new SimpleDateFormat("yyyy年MM月dd日 HH时mm分ss秒");
		return myFmt.format(new Date(timestamp));
	}
	
	public static String getTimeFormat(long timestamp, String format) {
		SimpleDateFormat myFmt = new SimpleDateFormat(format);
		return myFmt.format(new Date(timestamp));
	}
	
	public static String stringCookies(CookieStore cookies, String domain, String path) {
		Iterable<Cookie> it = cookies.get(true, domain, path);
		
		JsonArray stringcookies = new JsonArray();
		
		for (Cookie cookie : it) {
			JsonObject cookieObject = new JsonObject();
			
			cookieObject.put("name", cookie.name());
			cookieObject.put("lax", ServerCookieEncoder.LAX.encode(cookie));
			cookieObject.put("strict", ServerCookieEncoder.STRICT.encode(cookie));
			
			stringcookies.add(cookieObject);
		}
		
		return stringcookies.encode();
	}
	
	public static String generateCookies(List<String> setCookies) {
		StringBuffer sb = new StringBuffer();
		
		for (String setCookie : setCookies) {
			ServerCookieDecoder.LAX.decode(setCookie);
		}
		
		return sb.toString();
	}
	
    public static String generateUuid() {
    	char[] e = new char[36];
        for (int n = 0; n < 36; n++) {
        	double random = Math.floor(16 * Math.random());
        	int irandom = (int) random;
        	e[n] = "0123456789abcdef".substring(irandom, irandom + 1).charAt(0);
        }
        
        e[14] = '4';
        int i19 = 3 & e[19] | 8;
        e[19] = "0123456789abcdef".substring(i19, i19 + 1).charAt(0);
        e[8] = e[13] = e[18] = e[23] = '-';

        System.out.println(new String(e));
        
        return new String(e);
    }

    public static boolean isEmpty(String val) {
    	return val == null || "".equals(val.trim());
    }
}
