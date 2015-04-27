import sbt._
import Keys._
import sbtassembly.Plugin._
import AssemblyKeys._

object KafkaUtilsBuild extends Build {

  def sharedSettings = Defaults.defaultSettings ++ assemblySettings ++ Seq(
    version := "syx-0.1.0",
    scalaVersion := "2.8.0",
    organization := "com.syx",
    scalacOptions := Seq("-deprecation", "-unchecked", "-optimize"),
    unmanagedJars in Compile <<= baseDirectory map { base => (base / "lib" ** "*.jar").classpath },
    retrieveManaged := true,
    transitiveClassifiers in Scope.GlobalScope := Seq("sources"),
    resolvers ++= Seq(
      "sonatype-snapshots" at "http://oss.sonatype.org/content/repositories/snapshots",
      "sonatype-releases"  at "http://oss.sonatype.org/content/repositories/releases",
      "Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/",
      "JBoss Repository" at "http://repository.jboss.org/nexus/content/repositories/releases/"
    ),
    libraryDependencies ++= Seq(
      "log4j" % "log4j" % "1.2.17"
      //"org.scalatest" %% "scalatest" % "0.5.2" % "test"
	)
  )

  val slf4jVersion = "1.6.1"

//offsetmonitor project

  lazy val offsetmonitor = Project("offsetmonitor", file("."), settings = offsetmonSettings)

  def offsetmonSettings = sharedSettings ++ Seq(
  	  name := "KafkaOffsetMonitor",
	  libraryDependencies ++= Seq(
	  	"net.databinder" %% "unfiltered-filter" % "0.5.2",
		"net.databinder" %% "unfiltered-jetty" % "0.5.2",
		"net.databinder" %% "unfiltered-json" % "0.5.2",
		"com.quantifind" % "sumac_2.9.3" % "0.1",
        //"com.typesafe.slick" % "slick_2.10" % "2.0.0",
		"org.scalaquery" %% "scalaquery" % "0.9.0",
        "org.xerial" % "sqlite-jdbc" % "3.7.2",
		"com.twitter" % "util-core" % "3.0.0"
	  ),
	   resolvers ++= Seq(
	     "java m2" at "http://download.java.net/maven/2",
		 "twitter repo" at "http://maven.twttr.com"
	)
  )
}
