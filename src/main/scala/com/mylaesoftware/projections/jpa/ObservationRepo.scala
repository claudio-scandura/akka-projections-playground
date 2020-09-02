package com.mylaesoftware.projections.jpa

import java.sql.Connection

import javax.persistence.EntityManager
import org.postgresql.util.PSQLException
import org.slf4j.LoggerFactory

/**
 * create TABLE IF NOT EXISTS OBSERVATION (
 * value varchar(100) NOT NULL,
 * lastSeenAt bigint,
 * occurrences int,
 * primary key(value)
 * );
 */

class ObservationRepo {

  val logger = LoggerFactory.getLogger(classOf[ObservationRepo])

  private val updateQuery =
    """
      |INSERT INTO OBSERVATION as o (value, lastSeenAt, occurrences) VALUES (?value, ?lastSeenAt, 1)
      |ON CONFLICT (value) DO UPDATE SET lastSeenAt = excluded.lastSeenAt, occurrences = o.occurrences + 1
      |""".stripMargin

  def update(em: EntityManager, obs: Observation): Unit = {
    try {
      //      em.persist(obs)
      em.createNativeQuery(updateQuery)
        .setParameter("value", obs.value)
        .setParameter("lastSeenAt", obs.timestamp)
        .executeUpdate()
    } catch {
      case e: PSQLException => logger.error(s"Error while processing observation $e")
    }
  }


}
