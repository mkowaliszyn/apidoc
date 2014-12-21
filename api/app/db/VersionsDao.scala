package db

import com.gilt.apidoc.models.{Service, User, Version, Visibility}
import lib.VersionTag
import anorm._
import play.api.db._
import play.api.libs.json._
import play.api.Play.current
import java.util.UUID

case class VersionForm(
  json: String,
  visibility: Option[Visibility] = None
)

object VersionForm {
  import com.gilt.apidoc.models.json._
  implicit val versionFormReads = Json.reads[VersionForm]
}

object VersionsDao {

  private val LatestVersion = "latest"

  private val BaseQuery = """
    select versions.guid, versions.version, versions.json::varchar
     from versions
     join services on services.deleted_at is null and services.guid = versions.service_guid
    where versions.deleted_at is null
  """

  private val InsertQuery = """
    insert into versions
    (guid, service_guid, version, version_sort_key, json, created_by_guid)
    values
    ({guid}::uuid, {service_guid}::uuid, {version}, {version_sort_key}, {json}::json, {created_by_guid}::uuid)
  """

  def create(user: User, service: Service, version: String, json: String): Version = {
    val v = Version(guid = UUID.randomUUID,
                    version = version,
                    json = json)

    DB.withConnection { implicit c =>
      SQL(InsertQuery).on(
        'guid -> v.guid,
        'service_guid -> service.guid,
        'version -> v.version,
        'version_sort_key -> VersionTag(v.version).sortKey,
        'json -> v.json,
        'created_by_guid -> user.guid
      ).execute()
    }

    global.Actors.mainActor ! actors.MainActor.Messages.VersionCreated(v.guid)

    v
  }

  def softDelete(deletedBy: User, version: Version) {
    SoftDelete.delete("versions", deletedBy, version.guid)
  }

  def replace(user: User, version: Version, service: Service, newJson: String): Version = {
    DB.withTransaction { implicit c =>
      softDelete(user, version)
      VersionsDao.create(user, service, version.version, newJson)
    }
  }

  def findVersion(authorization: Authorization, orgKey: String, serviceKey: String, version: String): Option[Version] = {
    ServicesDao.findByOrganizationKeyAndServiceKey(authorization, orgKey, serviceKey).flatMap { service =>
      if (version == LatestVersion) {
        VersionsDao.findAll(authorization, serviceGuid = Some(service.guid), limit = 1).headOption
      } else {
        VersionsDao.findByServiceAndVersion(authorization, service, version)
      }
    }
  }

  def findByServiceAndVersion(authorization: Authorization, service: Service, version: String): Option[Version] = {
    VersionsDao.findAll(
      authorization,
      serviceGuid = Some(service.guid),
      version = Some(version),
      limit = 1
    ).headOption
  }

  def findByGuid(authorization: Authorization, guid: UUID): Option[Version] = {
    findAll(authorization, guid = Some(guid), limit = 1).headOption
  }

  def findAll(
    authorization: Authorization,
    serviceGuid: Option[UUID] = None,
    guid: Option[UUID] = None,
    version: Option[String] = None,
    limit: Long = 25,
    offset: Long = 0
  ): Seq[Version] = {
    val sql = Seq(
      Some(BaseQuery.trim),
      authorization.serviceFilter("services").map(v => "and " + v),
      guid.map { v => "and versions.guid = {guid}::uuid" },
      serviceGuid.map { _ => "and versions.service_guid = {service_guid}::uuid" },
      version.map { v => "and versions.version = {version}" },
      Some(s"order by versions.version_sort_key desc, versions.created_at desc limit ${limit} offset ${offset}")
    ).flatten.mkString("\n   ")

    val bind = Seq[Option[NamedParameter]](
      guid.map('guid -> _.toString),
      serviceGuid.map('service_guid -> _.toString),
      version.map('version ->_)
    ).flatten ++ authorization.bindVariables

    DB.withConnection { implicit c =>
      SQL(sql).on(bind: _*)().toList.map { row =>
        Version(
          guid = row[UUID]("guid"),
          version = row[String]("version"),
          json = row[String]("json")
        )
      }.toSeq
    }
  }

}
