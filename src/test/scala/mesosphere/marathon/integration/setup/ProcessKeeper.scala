package mesosphere.marathon.integration.setup

import scala.sys.ShutdownHookThread
import scala.sys.process._
import scala.util.{ Failure, Success, Try }
import com.google.inject.Guice
import org.rogach.scallop.ScallopConf
import com.google.common.util.concurrent.Service
import mesosphere.chaos.http.{ HttpService, HttpConf, HttpModule }
import mesosphere.chaos.metrics.MetricsModule
import java.io.File
import org.apache.log4j.Logger
import scala.concurrent.{ Await, Promise }
import scala.concurrent.duration._

/**
  * Book Keeper for processes and services.
  * During integration tests, several services and processes have to be launched.
  * The ProcessKeeper knows about them and can handle their lifecycle.
  */
object ProcessKeeper {

  private[this] val log = Logger.getLogger(getClass.getName)
  private[this] var processes = List.empty[Process]
  private[this] var services = List.empty[Service]

  def startHttpService(port: Int, assetPath: String) = {
    log.info(s"Start Http Service on port $port")
    val conf = new ScallopConf(Array("--http_port", port.toString, "--assets_path", assetPath)) with HttpConf
    conf.afterInit()
    val injector = Guice.createInjector(new MetricsModule, new HttpModule(conf), new IntegrationTestModule)
    val http = injector.getInstance(classOf[HttpService])
    services = http :: services
    http.start()
  }

  def startZooKeeper(port: Int, workDir: String) {
    val args = "org.apache.zookeeper.server.ZooKeeperServerMain" :: port.toString :: workDir :: Nil
    startJavaProcess("zookeeper", args, new File("."), sys.env, _.contains("binding to port"))
  }

  def startMesosLocal(): Process = startProcess("mesos", Process("mesos local"), _.toLowerCase.contains("registered with master"))

  def startMarathon(cwd: File, env: Map[String, String], arguments: List[String]): Process = {
    val argsWithMain = "mesosphere.marathon.Main" :: arguments
    startJavaProcess("marathon", argsWithMain, cwd, env, _.contains("Started SelectChannelConnector"))
  }

  def startJavaProcess(name: String, arguments: List[String], cwd: File, env: Map[String, String], upWhen: String => Boolean): Process = {
    log.info(s"Start java process $name with args: $arguments")
    val javaExecutable = sys.props.get("java.home").fold("java")(_ + "/bin/java")
    val classPath = sys.props.getOrElse("java.class.path", "target/classes")
    val builder = Process(javaExecutable :: "-classpath" :: classPath :: arguments, cwd, env.toList: _*)
    val process = startProcess(name, builder, upWhen)
    log.info(s"Java process $name up and running!")
    process
  }

  def startProcess(name: String, processBuilder: ProcessBuilder, upWhen: String => Boolean, timeout: Duration = 30.seconds): Process = {
    val up = Promise[Boolean]()
    val logger = new ProcessLogger {
      def checkUp(out: String) = {
        log.info(s"$name out: $out")
        if (!up.isCompleted && upWhen(out)) up.success(true)
      }
      override def buffer[T](f: => T): T = f
      override def out(s: => String) = checkUp(s)
      override def err(s: => String) = checkUp(s)
    }
    val process = processBuilder.run(logger)
    Try(Await.result(up.future, timeout)) match {
      case Success(_) => processes = process :: processes
      case Failure(_) =>
        process.destroy()
        throw new IllegalStateException(s"Process does not came up within time bounds ($timeout). Give up. $processBuilder")
    }
    process
  }

  def stopOSProcesses(grep: String): Unit = {
    val PIDRE = """\s*(\d+)\s.*""".r
    val processes = ("ps -x" #| s"grep $grep").!!.split("\n").map { case PIDRE(pid) => pid }
    processes.foreach(p => s"kill -9 $p".!)
  }

  def stopAllProcesses(): Unit = {
    processes.foreach(p => Try(p.destroy()))
    processes = Nil
  }

  def stopAllServices(): Unit = {
    services.foreach(s => Try(s.stop()))
    services = Nil
  }

  val shutDownHook: ShutdownHookThread = sys.addShutdownHook {
    stopAllProcesses()
    stopAllServices()
  }

  def main(args: Array[String]) {
    //startMarathon(new File("."), Map("MESOS_NATIVE_LIBRARY" -> "/usr/local/lib/libmesos.dylib"), List("--master", "local", "--event_subscriber", "http_callback"))
    startZooKeeper(2183, "/tmp/foo")
    Thread.sleep(10000)
    stopAllProcesses()
    //startHttpService(11211, ".")
  }
}

