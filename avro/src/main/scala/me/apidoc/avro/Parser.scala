package me.apidoc.avro

import java.io.File
import org.apache.avro.{Protocol, Schema}
import org.apache.avro.compiler.idl.Idl
import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer
import play.api.libs.json.{Json, JsValue}

private[avro] case class Builder() {

  val enums = ListBuffer[JsValue]()
  val models = ListBuffer[JsValue]()

}

sealed trait SchemaType {
  def parse(builder: Builder, schema: Schema): JsValue = {
    sys.error("Parse not supported")
  }
}

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

  case object Record extends SchemaType {

    override def parse(builder: Builder, schema: Schema): JsValue = {
      val fields = schema.getFields.map { field =>
        val (apidocType, required) = SchemaType.fromAvro(field.schema.getType).getOrElse {
          sys.error(s"Unsupported schema type[${field.schema.getType}] for field[${field.name}]")
        } match {
          case Array => sys.error("TODO")
          case Boolean => ("boolean", true)
          case Bytes => sys.error("apidoc does not support bytes type")
          case Double => ("double", true)
          case Enum => sys.error("TODO")
          case Fixed => ("decimal", true)
          case Float => ("double", true)
          case Int => ("integer", true)
          case Long => ("long", true)
          case Map => sys.error("TODO")
          case Null => ("unit", true)
          case String => ("string", true)
          case Union => sys.error("TODO")
          case Record => (field.schema.getName, true)
        }

        Json.obj(
          "name" -> field.name,
          "description" -> toOption(field.doc),
          "required" -> required,
          "type" -> apidocType
        )
      }

      Json.obj(
        "name" -> schema.getName,
        "description" -> toOption(schema.getDoc),
        "fields" -> fields
      )
    }
  }

  case object String extends SchemaType
  case object Union extends SchemaType

  val all = Seq(Array, Boolean, Bytes, Double, Enum, Fixed, Float, Int, Long, Map, Null, Record, String, Union)

  private[this]
  val byName = all.map(x => x.toString.toLowerCase -> x).toMap

  def fromAvro(avroType: org.apache.avro.Schema.Type) = fromString(avroType.toString)
  def fromString(value: String): Option[SchemaType] = byName.get(value.toLowerCase)

  def toOption(value: String): Option[String] = {
    if (value == null || value.trim.isEmpty) {
      None
    } else {
      Some(value.trim)
    }
  }
}

case class Parser() {

  val builder = Builder()

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

    SchemaType.fromAvro(schema.getType) match {
      case None => sys.error(s"Unsupported schema type[${schema.getType}]")
      case Some(st) => {
        println("PARSING st[$st]")
        val result = st.parse(builder, schema)
        println(result)
      }
    }
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
