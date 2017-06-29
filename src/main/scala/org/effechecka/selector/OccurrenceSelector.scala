package org.effechecka.selector

case class OccurrenceSelector(taxonSelector: String, wktString: String, traitSelector: String, uuid: Option[String] = None) {
  def withUUID: OccurrenceSelector = this.copy(uuid = Some(UuidUtils.uuidFor(this).toString))
}