module Codelists

  def self.fetch_calculation_basis
    {
      "VKP" => RDF::URI("http://data.rollvolet.be/calculation-basis/47c2570e-cc21-4496-ba76-7e89a3cf782d"),
      "Margin" => RDF::URI("http://data.rollvolet.be/calculation-basis/12c12fe4-9d88-4a63-9223-2c83d69da729")
    }
  end

  def self.fetch_categories
    categories_map = {}
    categories = Mu.query("SELECT ?label ?uri FROM <http://mu.semte.ch/graphs/rollvolet> WHERE { ?uri a <http://mu.semte.ch/vocabularies/ext/ProductCategory> ; <http://www.w3.org/2004/02/skos/core#prefLabel> ?label . }")
    categories.each { |solution| categories_map[solution[:label].value] = solution[:uri] }
    Mu.log.info "Build categories map #{categories_map.inspect}"
    categories_map
  end

  def self.fetch_unit_codes
    unit_codes_map = {}
    unit_codes = Mu.query("SELECT ?label ?uri FROM <http://mu.semte.ch/graphs/rollvolet> WHERE { ?uri a <http://mu.semte.ch/vocabularies/ext/UnitCode> ; <http://www.w3.org/2004/02/skos/core#prefLabel> ?label . }")
    unit_codes.each { |solution| unit_codes_map[solution[:label].value] = solution[:uri] }
    Mu.log.info "Build unit codes map #{unit_codes_map.inspect}"
    unit_codes_map
  end

  def self.fetch_departments
    departments_map = {}
    departments = Mu.query("SELECT ?label ?uri FROM <http://mu.semte.ch/graphs/rollvolet> WHERE { ?uri a <http://data.rollvolet.be/vocabularies/stock-management/WarehouseDepartment> ; <http://schema.org/name> ?label . }")
    departments.each { |solution| departments_map[solution[:label].value] = solution[:uri] }
    Mu.log.info "Build departments map #{departments_map.inspect}"
    departments_map
  end

  def self.fetch_business_entities
    business_entities_map = {}
    business_entities = Mu.query("SELECT ?label ?uri FROM <http://mu.semte.ch/graphs/rollvolet> WHERE { ?uri a <http://purl.org/goodrelations/v1#BusinessEntity> ; <http://purl.org/goodrelations/v1#name> ?label . }")
    business_entities.each { |solution| business_entities_map[solution[:label].value] = solution[:uri] }
    Mu.log.info "Build business entities map #{business_entities_map.inspect}"
    business_entities_map
  end
end
