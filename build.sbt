val monixVersion = "3.0.0-RC2"

addCommandAlias("ci",      ";+clean ;+test:compile ;+doc")
addCommandAlias("release", ";+clean ;+package ;+publishSigned ;sonatypeReleaseAll")

lazy val doNotPublishArtifact = Seq(
  publishArtifact := false,
  publishArtifact in (Compile, packageDoc) := false,
  publishArtifact in (Compile, packageSrc) := false,
  publishArtifact in (Compile, packageBin) := false
)

lazy val sharedSettings = Seq(
  organization := "io.monix",
  scalaVersion := "2.12.7",
  crossScalaVersions := Seq("2.11.12", "2.12.7"),

  scalacOptions ++= Seq(
    // warnings
    "-unchecked", // able additional warnings where generated code depends on assumptions
    "-deprecation", // emit warning for usages of deprecated APIs
    "-feature", // emit warning usages of features that should be imported explicitly
    // Features enabled by default
    "-language:higherKinds",
    "-language:implicitConversions",
    "-language:experimental.macros",
    // possibly deprecated options
    "-Ywarn-dead-code",
    "-Ywarn-inaccessible",
    "-language:higherKinds",
    "-language:existentials"
  ),

  // Force building with Java 8
  initialize := {
    val required = "1.8"
    val current  = sys.props("java.specification.version")
    assert(current == required, s"Unsupported build JDK: java.specification.version $current != $required")
  },

  // Targeting Java 6, but only for Scala <= 2.11
  javacOptions ++= (CrossVersion.partialVersion(scalaVersion.value) match {
    case Some((2, majorVersion)) if majorVersion <= 11 =>
      // generates code with the Java 6 class format
      Seq("-source", "1.6", "-target", "1.6")
    case _ =>
      // For 2.12 we are targeting the Java 8 class format
      Seq.empty
  }),

  // Targeting Java 6, but only for Scala <= 2.11
  scalacOptions ++= (CrossVersion.partialVersion(scalaVersion.value) match {
    case Some((2, majorVersion)) if majorVersion <= 11 =>
      // generates code with the Java 6 class format
      Seq("-target:jvm-1.6")
    case _ =>
      // For 2.12 we are targeting the Java 8 class format
      Seq.empty
  }),

  // version specific compiler options
  scalacOptions ++= (CrossVersion.partialVersion(scalaVersion.value) match {
    case Some((2, majorVersion)) if majorVersion >= 11 =>
      Seq(
        // Turns all warnings into errors ;-)
        "-Xfatal-warnings",
        // Enables linter options
        "-Xlint:adapted-args", // warn if an argument list is modified to match the receiver
        "-Xlint:nullary-unit", // warn when nullary methods return Unit
        "-Xlint:inaccessible", // warn about inaccessible types in method signatures
        "-Xlint:nullary-override", // warn when non-nullary `def f()' overrides nullary `def f'
        "-Xlint:infer-any", // warn when a type argument is inferred to be `Any`
        "-Xlint:missing-interpolator", // a string literal appears to be missing an interpolator id
        "-Xlint:doc-detached", // a ScalaDoc comment appears to be detached from its element
        "-Xlint:private-shadow", // a private field (or class parameter) shadows a superclass field
        "-Xlint:type-parameter-shadow", // a local type parameter shadows a type already in scope
        "-Xlint:poly-implicit-overload", // parameterized overloaded implicit methods are not visible as view bounds
        "-Xlint:option-implicit", // Option.apply used implicit view
        "-Xlint:delayedinit-select", // Selecting member of DelayedInit
        "-Xlint:by-name-right-associative", // By-name parameter of right associative operator
        "-Xlint:package-object-classes", // Class or object defined in package object
        "-Xlint:unsound-match" // Pattern match may not be typesafe
      )
    case _ =>
      Seq.empty
  }),

  // For warning of unused imports
  scalacOptions += "-Ywarn-unused-import",
  scalacOptions in (Compile, console) ~= {_.filterNot("-Ywarn-unused-import" == _)},
  scalacOptions in (Test, console) ~= {_.filterNot("-Ywarn-unused-import" == _)},

  scalacOptions in doc ++=
    Opts.doc.title(s"Monix"),
  scalacOptions in doc ++=
    Opts.doc.sourceUrl(s"https://github.com/monixio/monix-kafka/tree/v${version.value}€{FILE_PATH}.scala"),
  scalacOptions in doc ++=
    Seq("-doc-root-content", file("docs/rootdoc.txt").getAbsolutePath),
  scalacOptions in doc ++=
    Opts.doc.version(s"${version.value}"),

  // ScalaDoc settings
  autoAPIMappings := true,
  scalacOptions in ThisBuild ++= Seq(
    // Note, this is used by the doc-source-url feature to determine the
    // relative path of a given source file. If it's not a prefix of a the
    // absolute path of the source file, the absolute path of that file
    // will be put into the FILE_SOURCE variable, which is
    // definitely not what we want.
    "-sourcepath", file(".").getAbsolutePath.replaceAll("[.]$", "")
  ),

  parallelExecution in Test := false,
  parallelExecution in IntegrationTest := false,
  testForkedParallel in Test := false,
  testForkedParallel in IntegrationTest := false,
  concurrentRestrictions in Global += Tags.limit(Tags.Test, 1),

  licenses := Seq("APL2" -> url("http://www.apache.org/licenses/LICENSE-2.0.txt")),
  homepage := Some(url("https://github.com/monix/monix-kafka")),
  headerLicense := Some(HeaderLicense.Custom(
    """|Copyright (c) 2014-2018 by The Monix Project Developers.
       |
       |Licensed under the Apache License, Version 2.0 (the "License");
       |you may not use this file except in compliance with the License.
       |You may obtain a copy of the License at
       |
       |    http://www.apache.org/licenses/LICENSE-2.0
       |
       |Unless required by applicable law or agreed to in writing, software
       |distributed under the License is distributed on an "AS IS" BASIS,
       |WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
       |See the License for the specific language governing permissions and
       |limitations under the License."""
    .stripMargin)),

  scmInfo := Some(
    ScmInfo(
      url("https://github.com/monix/monix-kafka"),
      "scm:git@github.com:monix/monix-kafka.git"
    )),

  developers := List(
    Developer(
      id="alexelcu",
      name="Alexandru Nedelcu",
      email="noreply@alexn.org",
      url=url("https://alexn.org")
    ),
    Developer(
      id="pgawrys",
      name="Piotr Gawryś",
      email="pgawrys2@gmail.com",
      url=url("https://github.com/Avasil")
    )),

  // -- Settings meant for deployment on oss.sonatype.org
  publishMavenStyle := true,
  publishTo := {
    val nexus = "https://oss.sonatype.org/"
    if (isSnapshot.value)
      Some("snapshots" at nexus + "content/repositories/snapshots")
    else
      Some("releases"  at nexus + "service/local/staging/deploy/maven2")
  },

  publishArtifact in Test := false,
  pomIncludeRepository := { _ => false } // removes optional dependencies
)

def mimaSettings(projectName: String) = Seq(
  mimaPreviousArtifacts := Set("io.monix" %% projectName % "0.14")
)

lazy val commonDependencies = Seq(
  resolvers ++= Seq(
    "Typesafe Releases" at "http://repo.typesafe.com/typesafe/releases",
    Resolver.sonatypeRepo("releases")
  ),

  libraryDependencies ++= Seq(
    "io.monix" %% "monix-reactive" % monixVersion,
    "com.typesafe.scala-logging" %% "scala-logging" % "3.7.2",
    "com.typesafe" % "config" % "1.3.3",
    "org.slf4j" % "log4j-over-slf4j" % "1.7.25",
    // For testing ...
    "ch.qos.logback" % "logback-classic" % "1.1.3" % "test",
    "org.scalatest" %% "scalatest" % "3.0.3" % "test"
  )
)

lazy val monixKafka = project.in(file("."))
  .settings(sharedSettings)
  .settings(doNotPublishArtifact)
  .aggregate(kafka1x, kafka11, kafka10, kafka9)

lazy val kafka1x = project.in(file("kafka-1.0.x"))
  .settings(sharedSettings)
  .settings(commonDependencies)
  .settings(mimaSettings("monix-kafka-1x"))
  .settings(
    name := "monix-kafka-1x",
    libraryDependencies ++= Seq(
      "org.apache.kafka" %  "kafka-clients" % "1.0.2" exclude("org.slf4j","slf4j-log4j12") exclude("log4j", "log4j"),
      "net.manub"        %% "scalatest-embedded-kafka" % "1.0.0" % "test" exclude ("log4j", "log4j")
    )
  )

lazy val kafka11 = project.in(file("kafka-0.11.x"))
  .settings(sharedSettings)
  .settings(commonDependencies)
  .settings(mimaSettings("monix-kafka-11"))
  .settings(
    name := "monix-kafka-11",
    libraryDependencies ++= Seq(
      "org.apache.kafka" %  "kafka-clients" % "0.11.0.3" exclude("org.slf4j","slf4j-log4j12") exclude("log4j", "log4j"),
      "net.manub"        %% "scalatest-embedded-kafka" % "1.0.0" % "test" exclude ("log4j", "log4j")
    )
  )

lazy val kafka10 = project.in(file("kafka-0.10.x"))
  .settings(sharedSettings)
  .settings(commonDependencies)
  .settings(mimaSettings("monix-kafka-10"))
  .settings(
    name := "monix-kafka-10",
    libraryDependencies ++= Seq(
      "org.apache.kafka" %  "kafka-clients" % "0.10.2.2" exclude("org.slf4j","slf4j-log4j12") exclude("log4j", "log4j"),
      "net.manub"        %% "scalatest-embedded-kafka" % "0.13.1" % "test" exclude ("log4j", "log4j")
    )
  )

lazy val kafka9 = project.in(file("kafka-0.9.x"))
  .settings(sharedSettings)
  .settings(commonDependencies)
  .settings(mimaSettings("monix-kafka-9"))
  .settings(
    name := "monix-kafka-9",
    libraryDependencies ++= Seq(
      "org.apache.kafka" %  "kafka-clients" % "0.9.0.1" exclude("org.slf4j","slf4j-log4j12") exclude("log4j", "log4j")
    )
  )

//------------- For Release

enablePlugins(GitVersioning)

isSnapshot := version.value endsWith "SNAPSHOT"

/* The BaseVersion setting represents the previously released version. */
git.baseVersion := "0.14"

val ReleaseTag = """^v(\d+\.\d+(?:\.\d+(?:[-.]\w+)?)?)$""".r
git.gitTagToVersionNumber := {
  case ReleaseTag(v) => Some(v)
  case _ => None
}

git.formattedShaVersion := {
  val suffix = git.makeUncommittedSignifierSuffix(git.gitUncommittedChanges.value, git.uncommittedSignifier.value)

  git.gitHeadCommit.value map { _.substring(0, 7) } map { sha =>
    git.baseVersion.value + "-" + sha + suffix
  }
}
