package com.cody.recommand_test_recall.sparkCore.test

object demo {
  class A {
    def sound(){
      println("A sound")
    }
  }

  class B extends A{
    override def sound() ={
      println("B sound")
    }
  }

  class C extends B{
    override def sound()={
      print("C sounds")
    }
  }

  //其实， 这属于Scala泛型中的知识：上边界和下边界。
  // 上边界是“<:”，下边界是“>:”；T <: A的意思是：T必须是A的子类。
  // 这样一来，我们再看看这个函数的意思：定义了一个叫“PLAY”的函数，
  // 这个函数的参数必须传一个集合，一个什么样的集合呢？A 子类或者是Animal的集合（包含A）。
  // 函数右边就很好理解了，map中每个元素调用了sound方法

  def play[T <: A](things: Seq[T]) = things map (_.sound)

  //不能确定是否有sound的方法对吧，
  def play1[S >: A](things: Seq[S]) = things
  def main(args: Array[String]): Unit = {

    //    play(Seq(new A(),new B()))
    play1(Seq(new A(),new B())).map(_.sound())
  }
}
