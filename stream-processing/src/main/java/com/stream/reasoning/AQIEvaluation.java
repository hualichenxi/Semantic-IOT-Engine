package com.stream.reasoning;

import java.math.BigDecimal;

public class AQIEvaluation {
	public static void main(String[] args) {
		System.out.println(evaluate("PM10",78));
		System.out.println(evaluate("PM2p5",48));
		System.out.println(evaluate("O3",48));
		System.out.println(evaluate("CO",149));
		System.out.println(evaluate("SO2",56));
		System.out.println(evaluate("NO2",900));
		
	}

	public static int evaluate(String type,double c) {
		switch (type) {
		case "PM10":
			return calcAQIp(c, IAQI.PM10);
		case "PM2p5":
			return calcAQIp(c, IAQI.PM2p5);
		case "O3":
			return calcAQIp(c, IAQI.O3);
		case "CO":
			return calcAQIp(c, IAQI.CO);
		case "SO2":
			return calcAQIp(c, IAQI.SO2);
		case "NO2":
			return calcAQIp(c, IAQI.NO2);
		default:
			return -1;
		}
	}

	public static int calcAQIp(double c, AQImap[] type) {
		double IAQIhigh = 0, IAQIlow = 0, BPhigh = 0, BPlow = 0;
		BigDecimal C = new BigDecimal(c);

		boolean isOverflow = true;
		for (int i = 0; i < type.length; i++) {
			if (C.compareTo(new BigDecimal(type[i + 1].C)) <= 0) {
				BPhigh = type[i + 1].C;
				BPlow = type[i].C;
				IAQIhigh = type[i + 1].IAQI;
				IAQIlow = type[i].IAQI;
				isOverflow = false;
				break;
			}
		}

		if (isOverflow) {
			return 500;
		}

		double res = ((IAQIhigh - IAQIlow) / (BPhigh - BPlow)) * (c - BPlow)
				+ IAQIlow;

		return Integer.parseInt(String.format("%.0f", res));
	}
}

class AQImap {
	public int IAQI;
	public int C; // 浓度

	public AQImap(int iaqi, int c) {
		this.IAQI = iaqi;
		this.C = c;
	}
}

// 环境空气质量指数（AQI）技术规定.中华人民共和国环保部.
class IAQI {
	public static AQImap[] PM10 = { new AQImap(0, 0), new AQImap(50, 50),
			new AQImap(100, 150), new AQImap(150, 250), new AQImap(200, 350),
			new AQImap(300, 420), new AQImap(400, 500), new AQImap(500, 600), };

	public static AQImap[] PM2p5 = { new AQImap(0, 0), new AQImap(50, 35),
			new AQImap(100, 75), new AQImap(150, 115), new AQImap(200, 150),
			new AQImap(300, 250), new AQImap(400, 350), new AQImap(500, 500), };

	// O3 1小时平均 ug/m3
	public static AQImap[] O3 = { new AQImap(0, 0), new AQImap(50, 160),
			new AQImap(100, 200), new AQImap(150, 300), new AQImap(200, 400),
			new AQImap(300, 800), new AQImap(400, 1000), new AQImap(500, 1200), };

	// CO 1小时平均 mg/m3
	public static AQImap[] CO = { new AQImap(0, 0), new AQImap(50, 5),
			new AQImap(100, 10), new AQImap(150, 35), new AQImap(200, 60),
			new AQImap(300, 90), new AQImap(400, 120), new AQImap(500, 150), };

	// SO2 24小时平均 ug/m3
	public static AQImap[] SO2 = { new AQImap(0, 0), new AQImap(50, 50),
			new AQImap(100, 150), new AQImap(150, 475), new AQImap(200, 800),
			new AQImap(300, 1600), new AQImap(400, 2100),
			new AQImap(500, 2620), };

	// NO2 24小时平均 ug/m3
	public static AQImap[] NO2 = { new AQImap(0, 0), new AQImap(50, 40),
			new AQImap(100, 80), new AQImap(150, 180), new AQImap(200, 280),
			new AQImap(300, 565), new AQImap(400, 750), new AQImap(500, 940), };
}
