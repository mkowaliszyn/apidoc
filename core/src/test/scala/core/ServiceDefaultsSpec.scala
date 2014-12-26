package core

import org.scalatest.{FunSpec, Matchers}

class ServiceDefaultsSpec extends FunSpec with Matchers {

  it("accepts defaults for date-iso8601") {
    val json = """
    {
      "base_url": "http://localhost:9000",
      "name": "Api Doc",
      "models": {
        "user": {
          "fields": [
            { "name": "created_at", "type": "date-iso8601", "default": "2014-01-01" }
          ]
        }
      }
    }
    """
    val validator = ServiceValidator(json)
    validator.errors.mkString("") should be("")

    val createdAt = validator.serviceDescription.get.models.values.head.fields.find { _.name == "created_at" }.get
    createdAt.default should be(Some("2014-01-01"))
  }

  it("accepts strings and values as defaults for booleans") {
    val json = """
    {
      "base_url": "http://localhost:9000",
      "name": "Api Doc",
      "models": {
        "user": {
          "fields": [
            { "name": "is_active", "type": "boolean", "default": "true", "required": "true" },
            { "name": "is_athlete", "type": "boolean", "default": "false", "required": "false" }
          ]
        }
      }
    }
    """
    val validator = ServiceValidator(json)
    validator.errors.mkString("") should be("")

    val isActiveField = validator.serviceDescription.get.models.values.head.fields.find { _.name == "is_active" }.get
    isActiveField.default should be(Some("true"))
    isActiveField.required should be(Some(true))

    val isAthleteField = validator.serviceDescription.get.models.values.head.fields.find { _.name == "is_athlete" }.get
    isAthleteField.default should be(Some("false"))
    isAthleteField.required should be(Some(false))
  }

  it("rejects invalid boolean defaults") {
    val json = """
    {
      "base_url": "http://localhost:9000",
      "name": "Api Doc",
      "models": {
        "user": {
          "fields": [
            { "name": "is_active", "type": "boolean", "default": 1 }
          ]
        }
      }
    }
    """
    val validator = ServiceValidator(json)
    validator.errors.mkString("") should be("user.is_active Value[1] is not a valid boolean. Must be one of: true, false")
  }

  it("validates duplicate models in the resources section") {
    val json = """
    {
      "base_url": "http://localhost:9000",
      "name": "Api Doc",
      "models": {
        "user": {
          "fields": [
            { "name": "guid", "type": "uuid" }
          ]
        }
      },
      "resources": [
        {
          "model": "user",
          "operations": [
            {
              "method": "DELETE"
            }
          ]
        },
        {
          "model": "user",
          "operations": [
            {
              "method": "GET"
            }
          ]
        }
      ]
    }
    """
    val validator = ServiceValidator(json)
    validator.errors.mkString should be("Model[user] cannot be mapped to more than one resource")
  }

}
