package ts.training.transformer.configs

import com.typesafe.config.{Config, ConfigFactory}

import scala.util.{Failure, Success, Try}

trait ConfigSystem {
  implicit val config: Config = ConfigFactory.load()
  val ServicePrefix = "CV_"
  val appSettings: Config = config.getConfig("collection-validator")
  val reportMetrics: Boolean = appSettings.getBoolean("report-metrics")
  val metricsPort:Int = appSettings.getInt("metrics-port")

  def to_env_name(path: String): String = {
    val replaced = path.replace("-","_")
      .replace(".","_")
      .toUpperCase()
    s"$ServicePrefix$replaced"
  }

  def configOrEnv(path:String):String ={
    Try{
      config.getString(path)
    } match {
      case Success(value) => value
      case Failure(_) => sys.env.getOrElse(to_env_name(path),"")
    }
  }
}

