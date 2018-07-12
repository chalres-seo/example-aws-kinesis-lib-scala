package com.apps

object AppMain {
  def main(args: Array[String]): Unit = {
    require(args.length > 0, "need 1 argument. example type: library or api")
    args(0) match {
      case "library" => ExampleLibraryAppMain.main(args)
      case "api" => ExampleLibraryAppMain.main(args)
      case _ => println("need example type. library or api")
    }
  }
}
