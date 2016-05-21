package com.shouxinjk.ihealth.analyzer.util;

import java.util.UUID;

public class Util {
	public static String get32UUID() {
		String uuid = UUID.randomUUID().toString().trim().replaceAll("-", "");
		return uuid;
	}
}
