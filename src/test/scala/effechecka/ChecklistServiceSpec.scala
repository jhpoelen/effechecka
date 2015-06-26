package effechecka

import org.specs2.mutable.Specification
import spray.testkit.Specs2RouteTest
import spray.http._
import StatusCodes._

class ChecklistServiceSpec extends Specification with Specs2RouteTest with ChecklistService {
  def actorRefFactory = system
  
  "ChecklistService" should {
    "return instuctions for GET requests to the root path" in {
      Get() ~> myRoute ~> check {
        responseAs[String] must contain("/checklist?taxonSelector=Animalia,Insecta&amp;wktString=ENVELOPE(-150,-50,40,10)")
      }
    }
  }
}
