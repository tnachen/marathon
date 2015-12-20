package mesosphere.marathon.state

import java.lang.{ Integer => JInt }

import mesosphere.marathon.Protos
import org.apache.mesos.Protos.ContainerInfo
import org.apache.mesos.{ Protos => mesosproto }

import scala.collection.JavaConverters._
import scala.collection.immutable.Seq
import scala.util.Try

case class Container(
    `type`: mesosproto.ContainerInfo.Type = mesosproto.ContainerInfo.Type.DOCKER,
    volumes: Seq[Container.Volume] = Nil,
    docker: Option[Container.Docker] = None,
    mesos: Option[mesosproto.ContainerInfo.MesosInfo] = None) {

  def toProto(): Protos.ExtendedContainerInfo = {
    val builder = Protos.ExtendedContainerInfo.newBuilder
      .setType(`type`)
      .addAllVolumes(volumes.map(_.toProto).asJava)
    docker.foreach { d => builder.setDocker(d.toProto) }
    builder.build
  }

  def toMesos(): ContainerInfo = {
    val builder = ContainerInfo.newBuilder
      .setType(`type`)
      .addAllVolumes(volumes.map(_.toProto).asJava)
    docker.foreach { d => builder.setDocker(d.toMesos) }
    mesos.foreach { m => builder.setMesos(m) }
    builder.build
  }
}

object Container {

  object Empty extends Container

  def apply(proto: Protos.ExtendedContainerInfo): Container =
    Container(
      `type` = proto.getType,
      volumes = proto.getVolumesList.asScala.map(Container.Volume(_)).to[Seq],
      docker = Try(Docker(proto.getDocker)).toOption
    )

  /**
    * Lossy conversion for backwards compatibility with deprecated
    * container representation.
    */
  def apply(proto: mesosproto.CommandInfo.ContainerInfo): Container =
    Container(
      `type` = mesosproto.ContainerInfo.Type.DOCKER,
      docker = Some(Docker(proto.getImage))
    )

  /**
    * Lossy conversion for backwards compatibility with deprecated
    * container representation.
    */
  def apply(proto: Protos.ContainerInfo): Container =
    Container(
      `type` = mesosproto.ContainerInfo.Type.DOCKER,
      docker = Some(Docker(proto.getImage.toStringUtf8))
    )

  /**
    * A volume mapping either from host to container or vice versa.
    * Both paths can either refer to a directory or a file.  Paths must be
    * absolute.
    */
  case class Volume(
      containerPath: String,
      hostPath: String,
      mode: mesosproto.Volume.Mode) {
    def toProto(): mesosproto.Volume =
      mesosproto.Volume.newBuilder
        .setContainerPath(containerPath)
        .setHostPath(hostPath)
        .setMode(mode)
        .build
  }

  object Volume {
    def apply(proto: mesosproto.Volume): Volume =
      Volume(
        containerPath = proto.getContainerPath,
        hostPath = Option(proto.getHostPath).getOrElse(""),
        mode = proto.getMode
      )
  }

  /**
    * Docker-specific container parameters.
    */
  case class Docker(
      image: String = "",
      network: Option[mesosproto.ContainerInfo.DockerInfo.Network] = None,
      portMappings: Option[Seq[Docker.PortMapping]] = None,
      privileged: Boolean = false,
      parameters: Seq[Parameter] = Nil,
      forcePullImage: Boolean = false) {

    def toProto(): Protos.ExtendedContainerInfo.DockerInfo = {
      val builder = Protos.ExtendedContainerInfo.DockerInfo.newBuilder

      builder.setImage(image)

      network foreach builder.setNetwork

      portMappings.foreach { pms =>
        builder.addAllPortMappings(pms.map(_.toProto).asJava)
      }

      builder.setPrivileged(privileged)

      builder.addAllParameters(parameters.map(_.toProto).asJava)

      builder.setForcePullImage(forcePullImage)

      builder.build
    }

    def toMesos(): mesosproto.ContainerInfo.DockerInfo = {
      val builder = mesosproto.ContainerInfo.DockerInfo.newBuilder

      builder.setImage(image)

      network foreach builder.setNetwork

      portMappings.foreach { pms =>
        builder.addAllPortMappings(pms.map(_.toMesos).asJava)
      }

      builder.setPrivileged(privileged)

      builder.addAllParameters(parameters.map(_.toProto).asJava)

      builder.setForcePullImage(forcePullImage)

      builder.build
    }

  }

  object Docker {
    def apply(proto: Protos.ExtendedContainerInfo.DockerInfo): Docker =
      Docker(
        image = proto.getImage,

        network = if (proto.hasNetwork) Some(proto.getNetwork) else None,

        portMappings = {
          val pms = proto.getPortMappingsList.asScala

          if (pms.isEmpty) None
          else Some(pms.map(PortMapping(_)).to[Seq])
        },

        privileged = proto.getPrivileged,

        parameters = proto.getParametersList.asScala.map(Parameter(_)).to[Seq],

        forcePullImage = if (proto.hasForcePullImage) proto.getForcePullImage else false
      )

    /**
      * @param containerPort The container port to expose
      * @param hostPort      The host port to bind
      * @param servicePort   The well-known port for this service
      * @param protocol      Layer 4 protocol to expose (i.e. tcp, udp).
      */
    case class PortMapping(
        containerPort: JInt = 0,
        hostPort: JInt = 0,
        servicePort: JInt = 0,
        protocol: String = "tcp") {

      require(protocol == "tcp" || protocol == "udp", "protocol can only be 'tcp' or 'udp'")

      def toProto(): Protos.ExtendedContainerInfo.DockerInfo.PortMapping = {
        Protos.ExtendedContainerInfo.DockerInfo.PortMapping.newBuilder
          .setContainerPort(containerPort)
          .setHostPort(hostPort)
          .setProtocol(protocol)
          .setServicePort(servicePort)
          .build
      }

      def toMesos(): mesosproto.ContainerInfo.DockerInfo.PortMapping = {
        mesosproto.ContainerInfo.DockerInfo.PortMapping.newBuilder
          .setContainerPort(containerPort)
          .setHostPort(hostPort)
          .setProtocol(protocol)
          .build
      }
    }

    object PortMapping {
      def apply(proto: Protos.ExtendedContainerInfo.DockerInfo.PortMapping): PortMapping =
        PortMapping(
          proto.getContainerPort,
          proto.getHostPort,
          proto.getServicePort,
          proto.getProtocol
        )
    }

  }

}
