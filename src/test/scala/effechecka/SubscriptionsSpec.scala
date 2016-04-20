package effechecka

import java.net.URL

import com.typesafe.config.Config
import org.scalatest.{Matchers, WordSpec}


class SubscriptionsSpec extends WordSpec with Matchers with SubscriptionsCassandra with Configure {

  "Cassandra driver" should {
    "subscribe and unsubscribe" in {
      session.execute("TRUNCATE effechecka.subscriptions")
      val selector: OccurrenceSelector = OccurrenceSelector("someTaxa", "someWkt", "someTraits")
      val subscriptions = subscribersOf(selector)
      subscriptions.size should be(0)

      subscribe(new URL("mailto:john@doe"), selector) should be(new URL("mailto:john@doe"))

      val newSubscriptions: List[URL] = subscribersOf(selector)
      newSubscriptions.size should be(1)
      newSubscriptions should contain(new URL("mailto:john@doe"))

      unsubscribe(new URL("mailto:john@doe"), selector) should be(new URL("mailto:john@doe"))
      subscribersOf(selector).size should be(0)
    }
  }
}
