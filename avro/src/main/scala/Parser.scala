package lib

import scala.io.Source
import org.apache.avro.{Protocol, Schema}
import scala.collection.JavaConversions._

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

object Parser {

  //val is = Source.fromURL(getClass.getResource("/mobile-tapstream.avpr"))
  //println("RES: " + getClass.getResource("/mobile-tapstream.avpr"))

  private val schemaParser = {
    val p = new Schema.Parser()
    p.setValidate(true)
    p.setValidateDefaults(true)
    p
  }

  def parseSchema(path: String) {
    val schema = schemaParser.parse(new java.io.File(path))
    debug(schema)
  }

  private def debug(schema: Schema) {
    println("name: " + schema.getName)
    println("namespace: " + schema.getNamespace)
    println("fullName: " + schema.getFullName)
    println("type: " + schema.getType)

    SchemaType.fromAvro(schema.getType) match {
      case None => {
        sys.error(s"Unsupported schema type[${schema.getType}]")
      }
      case Some(SchemaType.Record) => {
        schema.getFields.foreach { field =>
          println("FIELD: " + field)
        }
      }
      case Some(other) => {
        println(s"TODO: Support $other")
      }
    }

    //schema.getTypes.foreach { t =>
    //  println(" T: " + t)
    //}
  }


  def parseProtocol(path: String) {
    println(s"parseProtocol($path)")
    val protocol = Protocol.parse(new java.io.File(path))
    println("name: " + protocol.getName)
    println("namespace: " + protocol.getNamespace)

    protocol.getTypes.foreach { schema =>
      println("--")
      debug(schema)
    }
  }

}
