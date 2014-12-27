package core

import com.gilt.apidocspec.models.Method
import org.scalatest.{FunSpec, Matchers}

class SvcIrisHubSpec extends FunSpec with Matchers {

  val Filenames = Seq("svc-iris-hub-0-0-1.json")
  val Dir = "core/src/test/resources"

  it("should parse valid json") {
    Filenames.foreach { name =>
      val validator = TestHelper.parseFile(s"${Dir}/${name}")
      if (!validator.isValid) {
        fail(s"Error parsing json file ${name}:\n  - " + validator.errors.mkString("\n  - "))
      }
    }
  }

  it("parses models") {
    val service = TestHelper.parseFile(s"${Dir}/svc-iris-hub-0-0-1.json").serviceDescription.get
    val modelNames = service.models.map(_.name)
    modelNames.contains("foo") should be(false)
    modelNames.contains("agreement") should be(true)
    modelNames.contains("vendor") should be(true)

    val item = service.models.find(_.name == "item").get
    item.fields.map(_.name).mkString(" ") should be("guid vendor_guid number quantity prices attributes return_policy metadata identifiers dimensions content images videos")
    item.fields.find(_.name == "number").get.`type` should be("string")
  }

  it("parses operations") {
    val service = TestHelper.parseFile(s"${Dir}/svc-iris-hub-0-0-1.json").serviceDescription.get
    val itemResource = service.resources.find(_.model.name == "item").getOrElse {
      sys.error("Could not find item resource")
    }

    val gets = itemResource.operations.filter(op => op.method == Method.Get && op.path == "/items")
    gets.size should be(1)
    gets.head.parameters.map(_.name).mkString(" ") should be("vendor_guid agreement_guid number limit offset")
    gets.head.responses.find(_.code == 200).get.`type` should be("[item]")

    val getsByGuid = itemResource.operations.filter(op => op.method == Method.Get && op.path == "/items/:guid")
    getsByGuid.size should be(1)
    getsByGuid.head.parameters.map(_.name).mkString(" ") should be("guid")
    getsByGuid.head.responses.map(_.code) should be(Seq(200))

    val deletes = itemResource.operations.filter(op => op.method == Method.Delete )
    deletes.size should be(1)
    deletes.head.parameters.map(_.name).mkString(" ") should be("guid")
    deletes.head.responses.map(_.code) should be(Seq(204))
  }

  it("all POST operations return either a 201 or a 409") {
    val service = TestHelper.parseFile(s"${Dir}/svc-iris-hub-0-0-1.json").serviceDescription.get
    service.resources.foreach { resource =>
      resource.operations.filter(_.method == Method.Post).foreach { op =>
        if (op.responses.map(_.code).toSeq.sorted != Seq(201, 409)) {
          fail("POST operation should return a 201 and a 409: " + op)
        }
      }
    }
  }

}
