package com.mylaesoftware.projections.jpa

import java.sql.Connection

import akka.japi.function
import akka.projection.jdbc.JdbcSession
import javax.persistence.EntityManager

class JpaSession(entityManager: EntityManager) extends JdbcSession {

  private val transaction = entityManager.getTransaction
  transaction.begin()

  override def withConnection[Result](func: function.Function[Connection, Result]): Result = {
    func(entityManager.unwrap(classOf[java.sql.Connection]))
  }

  def withEntityManager[Result](func: function.Function[EntityManager, Result]): Result = {
    func(entityManager)
  }

  override def commit(): Unit = transaction.commit()

  override def rollback(): Unit = if (transaction.isActive) transaction.rollback()

  override def close(): Unit = entityManager.close()
}