package me.apidoc.avro

import lib.Text

object Util {

  def formatName(name: String): String = {
    Text.camelCaseToUnderscore(name).toLowerCase
  }

  def toOption(value: String): Option[String] = {
    if (value == null || value.trim.isEmpty) {
      None
    } else {
      Some(value.trim)
    }
  }

}
