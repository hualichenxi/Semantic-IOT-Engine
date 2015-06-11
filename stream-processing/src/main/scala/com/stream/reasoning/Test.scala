package com.stream.reasoning

import java.math.BigDecimal

object Test {
  
  def bodyComfort(t: Double, h: Double, v: Double) = {
    val ssd = (1.818 * t + 18.18) * (0.88 + 0.002 * (h / 100)) + (t - 32) / (45 - t) - 3.2 * v + 18.2
    val bd = new BigDecimal(ssd)
    bd.setScale(0, BigDecimal.ROUND_HALF_UP).intValue()
  }
  
  def bodyCom(t: Double, h: Double)={
    val T=1.8*t+32
    val I=T-0.55*(1-h)*(T-58)
    I
  }

  def main(args: Array[String]){
    val t=32.0
    val h=53.0
    println(bodyComfort(t,h,0))
    println(bodyCom(t,h/100))
  }

}