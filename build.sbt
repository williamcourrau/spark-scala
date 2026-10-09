name := "scala-spark"
version := "0.1.0"
scalaVersion := "2.13.18"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "4.2.0"

// Spark needs its own JVM with these module flags on Java 17+
fork := true
run / connectInput := true
javaOptions ++= Seq(
  "java.lang", "java.lang.invoke", "java.lang.reflect", "java.io", "java.net",
  "java.nio", "java.util", "java.util.concurrent", "java.util.concurrent.atomic",
  "sun.nio.ch", "sun.nio.cs", "sun.security.action", "sun.util.calendar"
).map(p => s"--add-opens=java.base/$p=ALL-UNNAMED")
