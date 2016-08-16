import org.apache.log4j.Logger

/**
  * Created by lisa on 8/16/16.
  */
object LogHolder extends Serializable {
  @transient lazy val log = Logger.getLogger(getClass.getName)
}
