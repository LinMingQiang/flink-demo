package com.flink.scala.test

import org.apache.commons.codec.binary.Hex

import scala.io.Source

object Md5Test {

  /**
    * MD5加密
    *
    * @param s 输入字符串
    * @return MD5字符串
    */
  def encryptMd5_32(s: String): String = {
    val m = java.security.MessageDigest.getInstance("MD5")
    val b = s.getBytes("UTF-8")
    m.update(b, 0, b.length)
    val r = new java.math.BigInteger(1, m.digest()).toString(16)
    val sb = new StringBuffer()

    if (r.length == 32) {
      r
    } else {
      for (_ <- 0 until 32 - r.length) {
        sb.append("0")
      }
      sb.append(r)
      sb.toString
    }

  }

  def main(args: Array[String]): Unit = {
    test2()
//
//    val source = Source
//      .fromFile("/Users/eminem/Desktop/x.txt")
//      .getLines()
//      .map(x => {
//        (x, encryptMd5_32(x))
//      })
//    val idfa = Source
//      .fromFile("/Users/eminem/Desktop/idfa.txt")
//      .getLines()
//      .map(x => x -> 0)
//      .toMap
//    val imeimd5 = Source
//      .fromFile("/Users/eminem/Desktop/imeimd5.txt")
//      .getLines()
//      .map(x => x -> 0)
//      .toMap
//    val oiid = Source
//      .fromFile("/Users/eminem/Desktop/oaid.txt")
//      .getLines()
//      .map(x => x -> 0)
//      .toMap
//    var c = 0;
//    source.foreach {
//      case (src, md5) =>
//        if (idfa.contains(src.toUpperCase())) {
//          println("IDFA : ", src)
//          c += 1
//        } else if (oiid.contains(src.toLowerCase())) {
//          println("OAID : ", src)
//          c += 1
//        } else if (imeimd5.contains(md5)) {
//          println("MD5 : ", md5)
//          c += 1
//        }
//    }
//    println(c)
//    println(Hex.decodeHex(encryptMd5_32("ec:88:8f:81:a9:b2").toCharArray))
  }



  def test2(): Unit ={
    val s =
      s"""15900693917
         |13564312320
         |18616354527
         |13122179982
         |13661821549
         |18621895978
         |13621902453
         |18621281902
         |13524536495
         |13520054554
         |18018618896
         |13631421488
         |181 0109 8159
         |15221155660
         |13716846886
         |13611724824
         |18217603320
         |15901849846
         |13472789813
         |13761425362
         |18317174993
         |13818215957
         |15821954147
         |18380228792
         |13501649930
         |15221069154
         |15021309161
         |18297302879
         |18321280436
         |13810797654
         |15843880008
         |15800696369
         |18710150046
         |18756968525
         |15692152031
         |13818729354
         |18616691633
         |18516111770
         |13618492803
         |18200256289
         |17157808244
         |18918559778
         |18756058400
         |18701977963
         |18770092159
         |13501887628
         |15577310693
         |18810921210
         |15902052212
         |15210258439
         |13401161040
         |18611724209
         |18501010326
         |15010153015
         |16602077730
         |18819466706
         |15755166516
         |18656013431
         |15375240166""".stripMargin
    s.split("\n").map(encryptMd5_32).foreach(println)
  }


  def bidui(): Unit ={

  }
}
