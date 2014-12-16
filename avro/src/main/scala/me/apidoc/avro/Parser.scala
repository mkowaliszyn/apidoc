package me.apidoc.avro

import java.io.File
import org.apache.avro.{Protocol, Schema}
import org.apache.avro.compiler.idl.Idl
import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer
import play.api.libs.json.JsValue

sealed trait SchemaType
object SchemaType {

  case object Array extends SchemaType
  case object Boolean extends SchemaType
  case object Bytes extends SchemaType
  case object Double extends SchemaType
  case object Enum extends SchemaType
  case object Fixed extends SchemaType
  case object Float extends SchemaType
  case object Int extends SchemaType
  case object Long extends SchemaType
  case object Map extends SchemaType
  case object Null extends SchemaType
  case object Record extends SchemaType
  case object String extends SchemaType
  case object Union extends SchemaType

  val all = Seq(Array, Boolean, Bytes, Double, Enum, Fixed, Float, Int, Long, Map, Null, Record, String, Union)

  private[this]
  val byName = all.map(x => x.toString.toLowerCase -> x).toMap

  def fromAvro(avroType: org.apache.avro.Schema.Type) = fromString(avroType.toString)
  def fromString(value: String): Option[SchemaType] = byName.get(value.toLowerCase)

}

case class Parser() {

  private val enums = ListBuffer[JsValue]()
  private val models = ListBuffer[JsValue]()

  def parse(path: String) {
    println(s"parse($path)")

    val protocol = parseProtocol(path)
    println("name: " + protocol.getName)
    println("namespace: " + protocol.getNamespace)

    protocol.getTypes.foreach { schema =>
      println("--")
      parseSchema(schema)
    }
  }


  private def parseSchema(schema: Schema) {
    println("name: " + schema.getName)
    println("namespace: " + schema.getNamespace)
    println("fullName: " + schema.getFullName)
    println("type: " + schema.getType)

    SchemaType.fromAvro(schema.getType).getOrElse {
      sys.error(s"Unsupported schema type[${schema.getType}]")
    } match {
      case SchemaType.Record => {
        schema.getFields.foreach { field =>
          println("FIELD: " + field)
        }
      }
      case other => {
        println(s"TODO: Support $other")
      }
    }

    //schema.getTypes.foreach { t =>
    //  println(" T: " + t)
    //}
  }

  private def parseProtocol(
    path: String
  ): Protocol = {
    if (path.endsWith(".avdl")) {
      new Idl(new File(path)).CompilationUnit()

    } else if (path.endsWith(".avpr")) {
      Protocol.parse(new java.io.File(path))

    } else {
      sys.error("Unrecognized file extension for path[$path]")
    }
  }

}
