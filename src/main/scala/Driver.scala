import tools._
import scala.io.StdIn.readLine

object Driver {
  def main(args: Array[String]): Unit = {
    var run = true
    while(run){
      println("+++++++++++++++++++++++++++++")
      println("+ Main menu                 +")
      println("+ Nothing here yet          +")
      println("+ x - exit the program      +")
      println("+++++++++++++++++++++++++++++")
      println("Enter a menu option from the list:")
      val userEntry = readLine()
      userEntry match {
        case "x" => run = false
        case _ =>
      }
    }
  }
}
