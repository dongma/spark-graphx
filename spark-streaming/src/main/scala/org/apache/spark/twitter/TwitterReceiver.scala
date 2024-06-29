package org.apache.spark.twitter

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver
import twitter4j._

import java.io.{OutputStream, PrintStream}
import scala.concurrent.Promise

/**
 * Twitter接收数据
 *
 * @author Sam Ma
 * @date 2024/06/28
 */
class TwitterReceiver extends Receiver[Status](StorageLevel.MEMORY_ONLY) {
  import scala.concurrent.ExecutionContext.Implicits.global

  val twitterStreamPromise = Promise[TwitterStream]
  val twitterStreamFuture = twitterStreamPromise.future

  private def simpleStatusListener = new StatusListener {
    override def onStatus(status: Status): Unit = store(status)
    override def onDeletionNotice(statusDeletionNotice: StatusDeletionNotice): Unit = ()
    override def onTrackLimitationNotice(i: Int): Unit = ()
    override def onScrubGeo(l: Long, l1: Long): Unit = ()
    override def onStallWarning(stallWarning: StallWarning): Unit = ()
    override def onException(ex: Exception): Unit = ex.printStackTrace()
  }

  private def redirectSystemError() = System.setErr(new PrintStream(new OutputStream {
    override def write(b: Int): Unit = ()
  }))

  // run asynchronously
  override def onStart(): Unit = {
    redirectSystemError()

    // API有变化，相比例子中创建TwitterStream的方式,sample(en)  // call the Twitter sample endpoint for English tweets
    val twitterStream: TwitterStream = new TwitterStreamFactory("src/main/resources")
      .getInstance()
    twitterStream.addListener(simpleStatusListener)
    twitterStream.sample()

    twitterStreamPromise.success(twitterStream)
  }

  // run asynchronously
  override def onStop(): Unit = twitterStreamFuture.foreach { twitterStream =>
    twitterStream.cleanUp()
    twitterStream.shutdown()
  }

}
