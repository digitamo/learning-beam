lazy val root = project
  .in(file("."))
  .settings(
    name := "learning-beam-scala",
    version := "0.1.0",
    scalaVersion := "2.13.16",
    scalacOptions ++= Seq(
      "-deprecation",
      "-feature",
      "-unchecked",
      "-Xmacro-settings:show-coder-fallback=true" // Warn when Scio falls back to Kryo serialization
    ),
    libraryDependencies ++= Seq(
      // Scio: Spotify's Scala API for Apache Beam
      "com.spotify" %% "scio-core" % "0.14.8",
      "com.spotify" %% "scio-test" % "0.14.8" % Test,

      // Magnolify: automatic type class derivation for case classes
      "com.spotify" %% "magnolify-cats" % "0.7.4",

      // Cats: functional programming type classes (Eq, Show, etc.)
      "org.typelevel" %% "cats-core" % "2.12.0",

      // Direct runner for local execution (like Python's DirectRunner)
      "org.apache.beam" % "beam-runners-direct-java" % "2.59.0",

      // Quiet down Beam's verbose logging
      "org.slf4j" % "slf4j-simple" % "2.0.16"
    ),
    // Override snappy-java to a version available in local/remote caches
    dependencyOverrides += "org.xerial.snappy" % "snappy-java" % "1.1.10.7"
  )
