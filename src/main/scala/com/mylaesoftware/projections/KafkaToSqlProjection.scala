package com.mylaesoftware.projections

import java.lang
import java.util.UUID

import akka.actor.typed.{ActorSystem, Behavior}
import akka.projection.jdbc.scaladsl.{JdbcHandler, JdbcProjection}
import akka.projection.scaladsl.SourceProvider
import akka.projection._
import com.mylaesoftware.projections.KafkaToSqlProjection.{KafkaSource, LoggingObserver}
import com.mylaesoftware.projections.jpa.{JpaSession, Observation, ObservationRepo}
import javax.persistence.EntityManagerFactory
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.slf4j.{Logger, LoggerFactory}

class KafkaToSqlProjection(id: String,
                           source: KafkaSource[String, String],
                           observationRepo: ObservationRepo,
                           entityManagerFactory: EntityManagerFactory) extends LoggingObserver {

  override val logger = LoggerFactory.getLogger(classOf[KafkaToSqlProjection])

  private val sessionProvider = () => new JpaSession(entityManagerFactory.createEntityManager())


  private val dbHandler = () => new JdbcHandler[ConsumerRecord[String, String], JpaSession] {
    override def process(session: JpaSession, envelope: ConsumerRecord[String, String]): Unit = {
      session.withEntityManager { em =>
        observationRepo.update(em, new Observation(envelope.value, envelope.timestamp))
      }
    }
  }

  def behavior()(implicit system: ActorSystem[_]): Behavior[ProjectionBehavior.Command] = ProjectionBehavior {
    JdbcProjection
      .exactlyOnce(ProjectionId(classOf[KafkaToSqlProjection].getSimpleName, id + UUID.randomUUID().toString), source, sessionProvider, dbHandler)
      .withStatusObserver(this)
  }
}

object KafkaToSqlProjection {
  type KafkaSource[K, V] = SourceProvider[MergeableOffset[lang.Long], ConsumerRecord[K, V]]

  trait LoggingObserver extends StatusObserver[ConsumerRecord[String, String]] {
    def logger: Logger

    override def started(projectionId: ProjectionId): Unit = logger.info(s"Projection $projectionId started")

    override def failed(projectionId: ProjectionId, cause: Throwable): Unit = logger.info(s"Projection $projectionId failed")

    override def stopped(projectionId: ProjectionId): Unit = logger.info(s"Projection $projectionId stopped")

    override def beforeProcess(projectionId: ProjectionId, envelope: ConsumerRecord[String, String]): Unit = logger.info(s"Projection $projectionId processing $envelope")

    override def afterProcess(projectionId: ProjectionId, envelope: ConsumerRecord[String, String]): Unit = logger.info(s"Projection $projectionId processed $envelope")

    override def offsetProgress(projectionId: ProjectionId, env: ConsumerRecord[String, String]): Unit = logger.info(s"Projection $projectionId offset progressed for $env")

    override def error(projectionId: ProjectionId, env: ConsumerRecord[String, String], cause: Throwable, recoveryStrategy: HandlerRecoveryStrategy): Unit = logger.info(s"Projection $projectionId error while processing $env. $cause, $recoveryStrategy")
  }
}

