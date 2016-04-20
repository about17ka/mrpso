package mrpso

import org.apache.spark.util.random
import org.apache.spark.{SparkContext, SparkConf}

import scala.collection.mutable.ArrayBuffer
import scala.util.Random
import scala.math._

/**
 * Created by sponge苏苏 on 2016/4/20.
 */
object pso
{
  val c1 = 1.toDouble	//加速度
  val c2 = 1.toDouble	//加速度
  val w = 1.toDouble  //惯性系数
  val sizepop = 20.toInt;	//粒子数
  val dim = 2.toInt;	//优化函数的维数
  var m_dFitness = Double.MaxValue
  var m_a = ""
  var min = Double.MaxValue
  var gbest_temp = new ArrayBuffer[Double]  //临时全局最优解的位置

  def main(args: Array[String]): Unit = {

    var input = "G:\\testdata\\file.txt"
    var output = "G:\\testdata\\0\\"
    //System.out.println("Hello World!");
    val sc = new SparkContext("local", "helloworld")
    val step = 5 //做5次迭代
    for (i <- 0 until step) {
      print("第" + i + "次:" + i)
      val textfile = sc.textFile(input)
      val particle = textfile.map(line => line.split(" "))
      val num = particle.map(_.map(_.toDouble))
      val rdd = num.map(updatePbest)
      rdd.foreach(jobReducer2)
      val res = rdd.map(jobReducer3)
      res.map(x => x._1 + " " + x._2).saveAsTextFile(output) //去除在saveastextfile中烦人的括号
      var j = i + 1
      input = "G:\\testdata\\" + i.toString + "\\part-00000"
      output = "G:\\testdata\\" + j.toString + "\\"
    }
    sc.stop
  }

  def updatePbest(array:Array[Double])=
  {
    var pop = new ArrayBuffer[Double]   //粒子的位置
    var V = new ArrayBuffer[Double]   //粒子的速度
    var dpbest = new ArrayBuffer[Double]    //粒子本身的最优位置
    var gbest = new ArrayBuffer[Double]   //粒子的全局最优解
    var fitness = new ArrayBuffer[Double]
    var S_value = new ArrayBuffer[String]   //global best fitness保留全局最优值的坐标
    var F_value =""
    var bestfitness = 0.toDouble
    var m_dFitness = 0.toDouble   //飞行后的粒子适应值


    var g = 0.toDouble
    var sum1 = 0.toDouble
    var sum2 = 0.toDouble
    var k =1	//k赋1的原因是第0号元素为编号

    for(i <- 0 until dim)
    {
      pop += array(k)
      k = k + 1
      V += array(k)
      k = k + 1
      dpbest += array(k)
      k = k + 1
      gbest += array(k)
      k = k + 1
    }
    bestfitness = array(k)
    k = k + 1
    g = array(k)
    k = k + 1
    //分别给出粒子的各数数数
    //粒子的位置*2 + 粒子的速度*2 + 粒子本身的最优解的位置*2 + 粒子记录全局最优解的位置*2 + 局部最优解 + 全局最优解 总共10个数

    for(j <- 0 until dim)
    {
      V(j) = w * V(j) + c1 * Random.nextDouble() * (dpbest(j) - pop(j)) + c2 * Random.nextDouble() * (dpbest(j) - pop(j))
      pop(j) = pop(j) + V(j)
      //向着粒子本身最优解的位置飞行
    }
    //计算Ackley 函数的值
    for(l <- 0 until dim)
    {
      sum1 += pop(l) * pop(l)
      sum2 += Math.cos(2 * Math.PI * pop(l))
    }
    //m_dFitness 计算出的当前值
    m_dFitness= -20 * Math.exp(-0.2 * Math.sqrt((1.0 / dim) * sum1)) - Math.exp((1.0 / dim) * sum2) + 20 + 2.72
    if(m_dFitness < 0)
    {
      System.out.println(sum1 + " "+ m_dFitness + " " + sum2)
    }
    //飞行后的粒子适应值与粒子本身记录的最优解进行比较
    //如果飞行后的粒子适应值优于粒子本身记录的最优解，则更新粒子本身记录的最优解及最优解位置

    if(m_dFitness < bestfitness)
    {
      bestfitness = m_dFitness
      for (i <- 0 until dim)
      {
        dpbest(i) = pop(i)
      }
    }
    for (j <- 0 until dim)
    {
      S_value += pop(j).toString + " " + V(j) + " " + dpbest(j) + " "
      //S_value中存放着粒子位置，速度以及最优解位置信息的一半
    }
    for (k <- 0 until dim)
    {
      F_value += S_value(k)
      //F_value中存放着粒子位置，速度以及最优解位置信息的全部
    }
    //bestfitness
    F_value += m_dFitness.toString
    //注意这里和hadoop中不同，需要把m_dFitness传入到value中去
    new Tuple2(bestfitness:Double,F_value:String)
  }

  def jobReducer2(a:(Double,String)) =
  {

    var pop = new ArrayBuffer[Double]   //粒子的位置
    var V = new ArrayBuffer[Double]   //粒子的速度
    var dpbest = new ArrayBuffer[Double]    //粒子本身的最优位置
    if (a._1 < min)
    {
      min = a._1
      val itrr = a._2.split(" ")
      val itr = for (elem <- itrr) yield elem.toDouble
      var k = 0
      for (i <- 0 until dim)    //先还原出具有相同最优解的粒子的位置，速度以及最优解所在位置
      {
        pop += itr(k)
        k = k + 1
        V += itr(k)
        k = k + 1
        dpbest += itr(k)
        k = k + 1
      }

      for (i <- 0 until dim)
      {
        gbest_temp += dpbest(i)
      }
    }
  }

  def jobReducer3(a:(Double,String)) =
  {
    var pop = new ArrayBuffer[Double]   //粒子的位置
    var V = new ArrayBuffer[Double]   //粒子的速度
    var dpbest = new ArrayBuffer[Double]    //粒子本身的最优位置
    var gbest = new ArrayBuffer[Double]   //粒子的全局最优解
    var fitness = new ArrayBuffer[Double]
    var S_value = new ArrayBuffer[String]   //global best fitness保留全局最优值的坐标
    var F_value =""
    var bestfitness = 0.toDouble
    var m_dFitness = 0.toDouble   //飞行后的粒子适应值


    var g = 0.toDouble
    val itrr = a._2.split(" ")
    val itr = for (elem <- itrr) yield elem.toDouble
    var k = 0
    for (i <- 0 until dim)    //先还原出具有相同最优解的粒子的位置，速度以及最优解所在位置
    {
      pop += itr(k)
      k = k + 1
      V += itr(k)
      k = k + 1
      dpbest += itr(k)
      k = k + 1
    }
    m_dFitness = itr(k)
    bestfitness = min
    //此时bestfitness为全局最优解，被所有粒子所获取
    //var j = 0
    for (i <- 0 until dim)  //将全局最优解位置设置为粒子所记录的最优解位置,同时将粒子所记录的最优解位置赋给gbest_temp
    {
      gbest += gbest_temp(i)
    }
    for (j <- 0 until dim)
    {
      S_value += pop(j).toString + " " + V(j).toString + " " + dpbest(j).toString + " " + gbest(j).toString + " "
    }
    for (k <- 0 until dim)
    {
      F_value += S_value(k)
    }
    //还是将粒子的位置，速度，局部最优解和全局最优解记录到F_value中
    F_value += bestfitness.toString + " " + m_dFitness.toString
    //还要加上局部最优解和全局最优解的值

    new Tuple2(1:Double,F_value:String)
  }
}
