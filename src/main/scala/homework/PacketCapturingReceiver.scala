package homework

import jpcap.JpcapCaptor
import jpcap.packet.{IPPacket, Packet}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver

class PacketCapturingReceiver()
  extends Receiver[IPPacket](StorageLevel.DISK_ONLY) {

  def onStart() {
    new Thread("Packet Captor") {
      override def run() {
        receive()
      }
    }.start()
  }

  def onStop() {
  }


  private def receive() {
    var captor: JpcapCaptor = null
    try {
      val devices = JpcapCaptor.getDeviceList()
      captor = JpcapCaptor.openDevice(devices(0), -1, true, -1)
      captor.setFilter("ip and tcp", false);
      while (true) {
        val packet = captor.getPacket()
        packet match {
          case ippacket: IPPacket => store(ippacket)
          case _ => None
        }
      }
    } catch {
      case t: Throwable =>
        println("Packet capturing interrupted", t)
    } finally {
      if (captor != null) {
        captor.close()
      }
    }
  }
}