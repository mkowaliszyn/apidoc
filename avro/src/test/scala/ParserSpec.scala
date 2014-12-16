package lib

import org.scalatest.{FunSpec, Matchers}

class ParserSpec extends FunSpec with Matchers {

  it("parses") {
    //Parser.parse("avro/src/test/resources/mobile-tapstream.avpr")
    //Parser.parseSchema("avro/src/test/resources/simple.avpr")
    //Parser.parseProtocol("avro/src/test/resources/simple-protocol.avpr")
    Parser.parseProtocol("avro/src/test/resources/simple-protocol-with-gfc.avpr")
    //Parser.parseProtocol("avro/src/test/resources/mobile-tapstream.avpr")
  }

}
