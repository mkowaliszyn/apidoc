package core

import lib.{Primitives, Text}
import play.api.libs.json._

/**
 * Just parses json with minimal validation - build to provide a way to
 * generate meaningful validation messages back to user. Basic flow
 *
 * JSON => InternalService => Service
 *
 */
private[core] object InternalServiceForm {

  def apply(apiJson: String): InternalServiceForm = {
    val jsValue = Json.parse(apiJson)
    InternalServiceForm(jsValue)
  }

}

private[core] case class InternalServiceForm(json: JsValue) {

  lazy val name = (json \ "name").asOpt[String]
  lazy val baseUrl = (json \ "base_url").asOpt[String]
  lazy val basePath = (json \ "base_path").asOpt[String]
  lazy val description = (json \ "description").asOpt[String]

  lazy val models: Seq[InternalModelForm] = {
    (json \ "models").asOpt[JsValue] match {
      case Some(models: JsObject) => {
        models.fields.flatMap { v =>
          v match {
            case(key, value) => value.asOpt[JsObject].map(InternalModelForm(key, _))
          }
        }
      }
      case _ => Seq.empty
    }
  }

  lazy val enums: Seq[InternalEnumForm] = {
    (json \ "enums").asOpt[JsValue] match {
      case Some(enums: JsObject) => {
        enums.fields.flatMap { v =>
          v match {
            case(key, value) => value.asOpt[JsObject].map(InternalEnumForm(key, _))
          }
        }
      }
      case _ => Seq.empty
    }
  }

  lazy val headers: Seq[InternalHeaderForm] = {
    (json \ "headers").asOpt[JsArray].map(_.value).getOrElse(Seq.empty).flatMap { el =>
      el match {
        case o: JsObject => {
          Some(
            InternalHeaderForm(
              name = JsonUtil.asOptString(o, "name"),
              datatype = JsonUtil.asOptString(o, "type").map(InternalDatatype(_)),
              required = JsonUtil.asOptBoolean(o \ "required").getOrElse(true),
              description = JsonUtil.asOptString(o, "description"),
              default = JsonUtil.asOptString(o, "default")
            )
          )
        }
        case _ => None
      }
    }
  }

  lazy val resources: Seq[InternalResourceForm] = {
    (json \ "resources").asOpt[JsValue] match {
      case None => Seq.empty

      // Array is deprecated - JsObject is preferred
      case Some(resources: JsArray) => {
        resources.value.flatMap { v =>
          v match {
            case o: JsObject => {
              val modelName = (o \ "model").asOpt[String]
              Some(InternalResourceForm(modelName, models, o))
            }
            case _ => None
          }
        }
      }

      case Some(resources: JsObject) => {
        resources.fields.map { v =>
          v match {
            case(modelName, value) => InternalResourceForm(Some(modelName), models, value.as[JsObject])
          }
        }
      }

      case _ => Seq.empty
    }
  }

  lazy val typeResolver = TypeResolver(
    enumNames = enums.map(_.name),
    modelNames = models.map(_.name)
  )

  lazy val typeValidator = TypeValidator(
    enums = enums.map(e => TypeValidatorEnums(e.name, e.values.filter(!_.name.isEmpty).map(_.name.get)))
  )
}

case class InternalModelForm(
  name: String,
  plural: String,
  description: Option[String],
  fields: Seq[InternalFieldForm]
)

case class InternalEnumForm(
  name: String,
  description: Option[String],
  values: Seq[InternalEnumValueForm]
)

case class InternalEnumValueForm(
  name: Option[String],
  description: Option[String]
)

case class InternalHeaderForm(
  name: Option[String],
  datatype: Option[InternalDatatype],
  required: Boolean,
  description: Option[String],
  default: Option[String]
)

case class InternalResourceForm(
  modelName: Option[String],
  description: Option[String],
  path: String,
  operations: Seq[InternalOperationForm]
)

case class InternalOperationForm(
  method: Option[String],
  path: String,
  description: Option[String],
  namedPathParameters: Seq[String],
  parameters: Seq[InternalParameterForm],
  body: Option[InternalBodyForm],
  responses: Seq[InternalResponseForm],
  warnings: Seq[String] = Seq.empty
) {

  lazy val label = "%s %s".format(method.getOrElse(""), path).trim

}

case class InternalFieldForm(
  name: Option[String] = None,
  datatype: Option[InternalDatatype] = None,
  description: Option[String] = None,
  required: Boolean = true,
  default: Option[String] = None,
  example: Option[String] = None,
  minimum: Option[Long] = None,
  maximum: Option[Long] = None,
  warnings: Seq[String] = Seq.empty
)

case class InternalParameterForm(
  name: Option[String] = None,
  datatype: Option[InternalDatatype] = None,
  description: Option[String] = None,
  required: Boolean,
  default: Option[String] = None,
  example: Option[String] = None,
  minimum: Option[Long] = None,
  maximum: Option[Long] = None
)

case class InternalBodyForm(
  datatype: Option[InternalDatatype] = None,
  description: Option[String] = None
)

case class InternalResponseForm(
  code: String,
  datatype: Option[InternalDatatype] = None,
  warnings: Seq[String] = Seq.empty
) {

  lazy val datatypeLabel: Option[String] = datatype.map(_.label)

}

object InternalModelForm {

  def apply(name: String, value: JsObject): InternalModelForm = {
    val description = (value \ "description").asOpt[String]
    val plural: String = (value \ "plural").asOpt[String].getOrElse( Text.pluralize(name) )

    val fields = (value \ "fields").asOpt[JsArray] match {

      case None => Seq.empty

      case Some(a: JsArray) => {
        a.value.map { json => InternalFieldForm(json.as[JsObject]) }
      }

    }

    InternalModelForm(
      name = name,
      plural = plural,
      description = description,
      fields = fields
    )
  }

}

object InternalEnumForm {

  def apply(name: String, value: JsObject): InternalEnumForm = {
    val description = (value \ "description").asOpt[String]
    val values = (value \ "values").asOpt[JsArray] match {
       case None => Seq.empty
       case Some(a: JsArray) => {
         a.value.map { json =>
           InternalEnumValueForm(
             name = (json \ "name").asOpt[String],
             description = (json \ "description").asOpt[String]
           )
         }
       }
    }

    InternalEnumForm(
      name = name,
      description = description,
      values = values
    )
  }

}

object InternalResourceForm {

  def apply(modelName: Option[String], models: Seq[InternalModelForm], value: JsObject): InternalResourceForm = {
    val path = (value \ "path").asOpt[String].getOrElse {
      models.find(m => Some(m.name) == modelName) match {
        case Some(model: InternalModelForm) => "/" + model.plural
        case None => "/"
      }
    }

    val operations = (value \ "operations").asOpt[JsArray] match {
      case None => Seq.empty
      case Some(a: JsArray) => {
        a.value.map { json => InternalOperationForm(path, json.as[JsObject]) }
      }
    }

    InternalResourceForm(
      modelName = modelName,
      description = (value \ "description").asOpt[String],
      path = path,
      operations = operations
    )
  }

}

object InternalOperationForm {

  private val NoContentResponse = InternalResponseForm(code = "204", datatype = Some(InternalDatatype("unit")))

  def apply(resourcePath: String, json: JsObject): InternalOperationForm = {
    val path = resourcePath + (json \ "path").asOpt[String].getOrElse("")
    val namedPathParameters = Util.namedParametersInPath(path)
    val parameters = (json \ "parameters").asOpt[JsArray] match {
      case None => Seq.empty
      case Some(a: JsArray) => {
        a.value.map { data => InternalParameterForm(data.as[JsObject]) }
      }
    }

    val responses: Seq[InternalResponseForm] = {
      (json \ "responses").asOpt[JsObject] match {
        case None => {
          Seq(NoContentResponse)
        }

        case Some(responses: JsObject) => {
          responses.fields.map {
            case(code, value) => {
              value match {
                case o: JsObject => InternalResponseForm(code, o)
                case other => {
                  InternalResponseForm(
                    code = code,
                    warnings = Seq("Value must be a JsObject")
                  )
                }
              }
            }
          }
        }
      }
    }

    var warnings: Seq[String] = Seq.empty

    val body = (json \ "body") match {
      case o: JsObject => {
        Some(
          InternalBodyForm(
            datatype = JsonUtil.asOptString(o, "type").map(InternalDatatype(_)),
            description = (o \ "description").asOpt[String]
          )
        )
      }
      case u: JsUndefined => None
      case v: JsValue => {
        warnings = Seq(s"""body declaration must be an object, e.g. { "type": $v }""")
        None
      }
    }

    InternalOperationForm(
      method = (json \ "method").asOpt[String].map(_.toUpperCase),
      path = path,
      body = body,
      description = (json \ "description").asOpt[String],
      responses = responses,
      namedPathParameters = namedPathParameters,
      parameters = parameters,
      warnings = warnings
    )
  
  }
}

object InternalResponseForm {

  def apply(code: String, json: JsObject): InternalResponseForm = {
    InternalResponseForm(
      code = code,
      datatype = JsonUtil.asOptString(json, "type").map(InternalDatatype(_))
    )
  }
}

object InternalFieldForm {

  def apply(json: JsObject): InternalFieldForm = {
    val warnings = if (JsonUtil.hasKey(json, "enum") || JsonUtil.hasKey(json, "values")) {
      Seq("Enumerations are now first class objects and must be defined in an explicit enum section")
    } else {
      Seq.empty
    }

    InternalFieldForm(
      name = (json \ "name").asOpt[String],
      datatype = JsonUtil.asOptString(json, "type").map(InternalDatatype(_)),
      description = (json \ "description").asOpt[String],
      required = JsonUtil.asOptBoolean(json \ "required").getOrElse(true),
      default = JsonUtil.asOptString(json, "default"),
      minimum = (json \ "minimum").asOpt[Long],
      maximum = (json \ "maximum").asOpt[Long],
      example = JsonUtil.asOptString(json, "example"),
      warnings = warnings
    )
  }

}

object InternalParameterForm {

  def apply(json: JsObject): InternalParameterForm = {
    InternalParameterForm(
      name = (json \ "name").asOpt[String],
      datatype = JsonUtil.asOptString(json, "type").map(InternalDatatype(_)),
      description = (json \ "description").asOpt[String],
      required = JsonUtil.asOptBoolean(json \ "required").getOrElse(true),
      default = JsonUtil.asOptString(json, "default"),
      minimum = (json \ "minimum").asOpt[Long],
      maximum = (json \ "maximum").asOpt[Long],
      example = JsonUtil.asOptString(json, "example")
    )
  }

}

/**
 * Parse numbers and string json values as strings
 */
private[core] object JsonUtil {

  def asOptString(json: JsValue, field: String): Option[String] = {
    val value = (json \ field)
    asOptString(value)
  }

  def asOptString(value: JsValue): Option[String] = {
    value match {
      case (_: JsUndefined) => None
      case (v: JsString) => Some(v.value)
      case (v: JsValue) => Some(v.toString)
    }
  }

  def asOptBoolean(value: JsValue): Option[Boolean] = {
    asOptString(value).map(_ == "true")
  }

  def hasKey(json: JsValue, field: String): Boolean = {
    (json \ field) match {
      case (_: JsUndefined) => false
      case _ => true
    }
  }
}

sealed trait InternalDatatype {

  def names: Seq[String]
  def label: String

  protected def makeLabel(prefix: String = "", postfix: String = ""): String = {
    prefix + names.mkString(" | ") + postfix
  }

}

private[core] object InternalDatatype {

  case class List(names: Seq[String]) extends InternalDatatype {
    override def label = makeLabel("[", "]")
  }

  case class Map(names: Seq[String]) extends InternalDatatype {
    override def label = makeLabel("map[", "]")
  }

  case class Option(names: Seq[String]) extends InternalDatatype {
    override def label = makeLabel("option[", "]")
  }

  case class Singleton(names: Seq[String]) extends InternalDatatype {
    override def label = makeLabel()
  }

  private val ListRx = "^\\[(.*)\\]$".r
  private val MapRx = "^map\\[(.*)\\]$".r
  private val OptionRx = "^option\\[(.*)\\]$".r
  private val DefaultMapRx = "^map$".r

  def apply(value: String): InternalDatatype = {
    value match {
      case ListRx(name) => InternalDatatype.List(parse(name))
      case MapRx(name) => InternalDatatype.Map(parse(name))
      case OptionRx(name) => InternalDatatype.Option(parse(name))
      case DefaultMapRx() => InternalDatatype.Map(parse("string"))
      case _ => InternalDatatype.Singleton(parse(value))
    }
  }

  private def parse(value: String): Seq[String] = value.split("\\|").map(_.trim)

}
