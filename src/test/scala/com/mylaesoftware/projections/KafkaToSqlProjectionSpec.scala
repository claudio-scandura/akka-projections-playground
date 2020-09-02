package com.mylaesoftware.projections

import java.sql.ResultSet
import java.util.UUID

import akka.actor.testkit.typed.scaladsl.ActorTestKit
import akka.actor.typed.ActorSystem
import akka.actor.typed.scaladsl.Behaviors
import akka.kafka.ConsumerSettings
import akka.projection.ProjectionBehavior
import akka.projection.kafka.scaladsl.KafkaSourceProvider
import com.mylaesoftware.projections.jpa.ObservationRepo
import com.opentable.db.postgres.embedded.EmbeddedPostgres
import javax.persistence.Persistence
import net.manub.embeddedkafka.{EmbeddedK, EmbeddedKafka, EmbeddedKafkaConfig}
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.{Eventually, IntegrationPatience}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

import scala.util.{Random, Success}

class KafkaToSqlProjectionSpec extends AnyWordSpecLike
  with Matchers
  with BeforeAndAfterAll
  with EmbeddedKafka
  with Eventually
  with IntegrationPatience {

  private var kafka: EmbeddedK = null
  private var postgres: EmbeddedPostgres = null

  implicit val kafkaConfig: EmbeddedKafkaConfig = EmbeddedKafkaConfig(kafkaPort = 9092)

  override protected def beforeAll(): Unit = {
    kafka = EmbeddedKafka.start()
    postgres = EmbeddedPostgres.builder()
      .setCleanDataDirectory(true)
      .setPort(5432)
      .start()
    setupDbSchema()
  }

  override protected def afterAll(): Unit = {
    kafka.stop(true)
    postgres.close()
  }

  private def setupDbSchema() {
    val connection = postgres.getPostgresDatabase.getConnection

    connection.createStatement().execute(
      """
        | drop TABLE IF EXISTS OBSERVATION;
        | create TABLE IF NOT EXISTS OBSERVATION (
        |  value varchar(100) NOT NULL,
        |  lastSeenAt bigint,
        |  occurrences int,
        |  primary key(value)
        | );
        |
        |
        | drop TABLE IF EXISTS "AKKA_PROJECTION_OFFSET_STORE";
        | create table if not exists "AKKA_PROJECTION_OFFSET_STORE" (
        |  "PROJECTION_NAME" VARCHAR(255) NOT NULL,
        |  "PROJECTION_KEY" VARCHAR(255) NOT NULL,
        |  "OFFSET" VARCHAR(255) NOT NULL,
        |  "MANIFEST" VARCHAR(4) NOT NULL,
        |  "MERGEABLE" BOOLEAN NOT NULL,
        |  "LAST_UPDATED" BIGINT NOT NULL
        | );
        |
        |
        | create index if not exists "PROJECTION_NAME_INDEX" on "AKKA_PROJECTION_OFFSET_STORE" ("PROJECTION_NAME");
        |
        | alter table "AKKA_PROJECTION_OFFSET_STORE" drop constraint if exists "PK_PROJECTION_ID";
        | alter table "AKKA_PROJECTION_OFFSET_STORE" add constraint "PK_PROJECTION_ID" primary key("PROJECTION_NAME","PROJECTION_KEY");
        |""".stripMargin)
  }

  "The projection" should {

    "consume Kafka message and update the DB" in {
      implicit val system = ActorSystem[ProjectionBehavior.Command](Behaviors.empty, "system")
      val testKit = ActorTestKit(system)
      val topicName = s"test-topic-${Random.nextInt(100)}"

      createCustomTopic(topicName) shouldBe a[Success[_]]

      val consumerSettings =
        ConsumerSettings(system, new StringDeserializer, new StringDeserializer)
          .withBootstrapServers(s"localhost:${kafkaConfig.kafkaPort}")
          .withGroupId(UUID.randomUUID().toString)
          .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

      val kafkaSource = KafkaSourceProvider(system, consumerSettings, Set(topicName))
      val emf = Persistence.createEntityManagerFactory("com.mylaesoftware.projections.postgres")
      val projection = new KafkaToSqlProjection(topicName, kafkaSource, new ObservationRepo, emf)


      val connection = postgres.getPostgresDatabase.getConnection

      val message = UUID.randomUUID().toString
      publishStringMessageToKafka(topicName, message)

      testKit.spawn(projection.behavior())

      eventually {
        connection.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE)
          .executeQuery(s"select * from observation where value = '$message'")
          .first() shouldBe true

      }
    }

    "Increment occurrences when the same message is processed twice" in {
      implicit val system = ActorSystem[ProjectionBehavior.Command](Behaviors.empty, "system")
      val testKit = ActorTestKit(system)
      val topicName = s"test-topic-${Random.nextInt(100)}"

      createCustomTopic(topicName) shouldBe a[Success[_]]

      val consumerSettings =
        ConsumerSettings(system, new StringDeserializer, new StringDeserializer)
          .withBootstrapServers(s"localhost:${kafkaConfig.kafkaPort}")
          .withGroupId(UUID.randomUUID().toString)
          .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

      val kafkaSource = KafkaSourceProvider(system, consumerSettings, Set(topicName))
      val emf = Persistence.createEntityManagerFactory("com.mylaesoftware.projections.postgres")
      val projection = new KafkaToSqlProjection(topicName, kafkaSource, new ObservationRepo, emf)

      testKit.spawn(projection.behavior())

      val message = UUID.randomUUID().toString
      publishStringMessageToKafka(topicName, message)
      publishStringMessageToKafka(topicName, message)


      val connection = postgres.getPostgresDatabase.getConnection
      eventually {
        val res = connection.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE)
          .executeQuery(s"select * from observation where value = '$message'")

        res.first() shouldBe true
        res.getInt("occurrences") shouldBe 2
      }
    }

    "Stop and restart from last committed offset" in {

      implicit val system = ActorSystem[ProjectionBehavior.Command](Behaviors.empty, "system")
      val testKit = ActorTestKit(system)
      val topicName = s"test-topic-${Random.nextInt(100)}"

      createCustomTopic(topicName) shouldBe a[Success[_]]

      val consumerSettings =
        ConsumerSettings(system, new StringDeserializer, new StringDeserializer)
          .withBootstrapServers(s"localhost:${kafkaConfig.kafkaPort}")
          .withGroupId(UUID.randomUUID().toString)
          .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

      val kafkaSource = KafkaSourceProvider(system, consumerSettings, Set(topicName))
      val emf = Persistence.createEntityManagerFactory("com.mylaesoftware.projections.postgres")
      val projection = new KafkaToSqlProjection(topicName, kafkaSource, new ObservationRepo, emf)

      val connection = postgres.getPostgresDatabase.getConnection

      val message = UUID.randomUUID().toString

      val projector = testKit.spawn(projection.behavior())
      publishStringMessageToKafka(topicName, message)

      eventually {
        connection.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE)
          .executeQuery(s"select * from observation where value = '$message'")
          .first() shouldBe true

      }

      projector ! ProjectionBehavior.Stop

      testKit.spawn(projection.behavior())

      val otherMessage = UUID.randomUUID().toString
      publishStringMessageToKafka(topicName, otherMessage)
      eventually {
        connection.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE)
          .executeQuery(s"select * from observation where value = '$otherMessage'")
          .first() shouldBe true

      }
    }
  }
}
