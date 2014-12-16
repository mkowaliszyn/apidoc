package me.apidoc.avro

import java.io.File
import org.apache.avro.{Protocol, Schema}
import org.apache.avro.compiler.idl.Idl
import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer
import play.api.libs.json.{Json, JsArray, JsBoolean, JsObject, JsString, JsValue}

private[avro] case class Builder() {

  private val enums = scala.collection.mutable.Map[String, JsValue]()
  private val models = scala.collection.mutable.Map[String, JsValue]()

  def toApiJson(
    name: String,
    baseUrl: String
  ): JsValue = {
    JsObject(
      Seq(
        Some("name" -> JsString(name)),
        Some("base_url" -> JsString(baseUrl)),
        enums.toList match {
          case Nil => None
          case data => Some("enums" -> JsObject(data))
        },
        models.toList match {
          case Nil => None
          case data => Some("models" -> JsObject(data))
        }
      ).flatten
    )
  }

  def addModel(name: String, description: Option[String], fields: Seq[JsValue]) {
    models += (name ->
      JsObject(
        Seq(
          description.map { v => "description" -> JsString(v) },
          Some("fields" -> JsArray(fields))
        ).flatten
      )
    )
  }

  def addEnum(name: String, description: Option[String], values: Seq[String]) {
    enums += (name ->
      JsObject(
        Seq(
          description.map { v => "description" -> JsString(v) },
          Some(
            "values" -> JsArray(values.map { value => Json.obj("name" -> value) })
          )
        ).flatten
      )
    )
  }

}

sealed trait SchemaType {
  def parse(builder: Builder, schema: Schema) {
    sys.error("Parse not supported: " + getClass)
  }
}

object Apidoc {

  case class Type(
    name: String,
    required: Boolean = true
  )

  def field(field: Schema.Field): JsValue = {
    val t = Apidoc.getType(field.schema)
    val doc = Util.toOption(field.doc)

    JsObject(
      Seq(
        Some("name" -> JsString(field.name)),
        t.required match {
          case true => None
          case false => Some("required" -> JsBoolean(false))
        },
        Some("type" -> JsString(t.name)),
        Util.toOption(field.doc).map { v => "description" -> JsString(v) }
      ).flatten
    )
  }

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
            if (SchemaType.fromAvro(t1.getType) == Some(SchemaType.Null)) {
              getType(t2).copy(required = false)

            } else if (SchemaType.fromAvro(t2.getType) == Some(SchemaType.Null)) {
              getType(t1).copy(required = false)

            } else {
              sys.error("apidoc does not support union types w/ more then 1 non null type: " + Seq(t1, t2).map(_.getType).mkString(", "))
            }
          }
          case types => {
            sys.error("apidoc does not support union types w/ more then 1 non null type: " + types.map(_.getType).mkString(", "))
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

  case object Enum extends SchemaType {
    override def parse(builder: Builder, schema: Schema) {
      builder.addEnum(
        name = schema.getName,
        description = Util.toOption(schema.getDoc),
        values = schema.getEnumSymbols
      )
    }
  }

  case object Fixed extends SchemaType
  case object Float extends SchemaType
  case object Int extends SchemaType
  case object Long extends SchemaType
  case object Map extends SchemaType
  case object Null extends SchemaType

  case object Record extends SchemaType {

    override def parse(builder: Builder, schema: Schema) {
      builder.addModel(
        name = schema.getName,
        description = Util.toOption(schema.getDoc),
        fields = schema.getFields.map(Apidoc.field(_))
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

}

case class Parser() {

  val builder = Builder()

  def parse(path: String) {
    println(s"parse($path)")

    val protocol = parseProtocol(path)
    println(s"protocol name[${protocol.getName}] namespace[${protocol.getNamespace}]")

    protocol.getTypes.foreach { schema =>
      parseSchema(schema)
    }

    println(
      builder.toApiJson(
        name = protocol.getName,
        baseUrl = "http://localhost"
      )
    )

  }


  private def parseSchema(schema: Schema) {
    println(s"schema name[${schema.getName}] namespace[${schema.getNamespace}]")

    SchemaType.fromAvro(schema.getType) match {
      case None => sys.error(s"Unsupported schema type[${schema.getType}]")
      case Some(st) => {
        st match {
          case SchemaType.Record | SchemaType.Enum => {
            st.parse(builder, schema)
          }
          case SchemaType.Fixed => {
            println("WARNING: skipping schema of type fixed")
          }
          case SchemaType.Array | SchemaType.Boolean | SchemaType.Bytes | SchemaType.Double | SchemaType.Float | SchemaType.Int | SchemaType.Long | SchemaType.Map | SchemaType.String | SchemaType.Union | SchemaType.Null => {
            sys.error(s"Unexpected avro type[$st]")
          }
        }
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
