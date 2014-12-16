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

object Apidoc {

  case class Type(
    name: String,
    required: Boolean = true
  )

  def getType(schema: Schema): Type = {
    SchemaType.fromAvro(schema.getType).getOrElse {
      sys.error(s"Unsupported schema type[${schema.getType}]")
    } match {
      case SchemaType.Array => Type("[%s]".format(getType(schema.getElementType).name))
      case SchemaType.Boolean => Type("boolean")
      case SchemaType.Bytes => sys.error("apidoc does not support bytes type")
      case SchemaType.Double => Type("double")
      case SchemaType.Enum => Type(schema.getName)
      case SchemaType.Fixed => Type("decimal")
      case SchemaType.Float => Type("double")
      case SchemaType.Int => Type("integer")
      case SchemaType.Long => Type("long")
      case SchemaType.Map => Type("map[%s]".format(getType(schema.getValueType).name))
      case SchemaType.Null => Type("unit")
      case SchemaType.String => Type("string")
      case SchemaType.Union => {
        schema.getTypes.toList match {
          case Nil => sys.error("union must have at least 1 type")
          case t :: Nil => getType(t)
          case t1 :: t2 :: Nil => {
            if (t1.getType == SchemaType.Null) {
              getType(t2).copy(required = false)

            } else if (t2.getType == SchemaType.Null) {
              getType(t1).copy(required = false)

            } else {
              sys.error("apidoc does not support union types: " + Seq(t1, t2).map(_.getType).mkString(", "))
            }
          }
          case types => {
            sys.error("apidoc does not support union types: " + types.map(_.getType).mkString(", "))
          }
        }
      }
      case SchemaType.Record => Type(schema.getName)
    }
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
        val t = Apidoc.getType(field.schema)

        Json.obj(
          "name" -> field.name,
          "description" -> toOption(field.doc),
          "required" -> t.required,
          "type" -> t.name
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
        println(s"PARSING st[$st]")
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
