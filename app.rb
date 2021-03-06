require "linkeddata"
require "bson"
require "pry-byebug"
require "csv"


###
# RDF Utils
###
FOAF = RDF::Vocab::FOAF
SKOS = RDF::Vocab::SKOS
DCT = RDF::Vocab::DC
PROV = RDF::Vocab::PROV
SCHEMA = RDF::Vocab::SCHEMA
GR = RDF::Vocab::GR
NFO = RDF::Vocabulary.new("http://www.semanticdesktop.org/ontologies/2007/03/22/nfo#")
NIE = RDF::Vocabulary.new("http://www.semanticdesktop.org/ontologies/2007/01/19/nie#")
DBPEDIA = RDF::Vocabulary.new("http://dbpedia.org/resource/")
MU = RDF::Vocabulary.new("http://mu.semte.ch/vocabularies/core/")
EXT = RDF::Vocabulary.new("http://mu.semte.ch/vocabularies/ext/")
STOCK = RDF::Vocabulary.new("http://data.rollvolet.be/vocabularies/stock-management/")
PRICE = RDF::Vocabulary.new("http://data.rollvolet.be/vocabularies/pricing/")

BASE_URI = "http://data.rollvolet.be/%{resource}/%{id}"


###
# I/O Configuration
###
class Configuration
  def initialize
    @input_dir = "data/input"
    @output_dir = "data/output"

    @input_files = {
      departments: "Afdeling.csv",
      products: "Artikel.csv",
      product_suppliers: "ArtikelLeverancier.csv",
      categories: "Categorie.csv",
      subcategories: "SubCategorie.csv",
      unit_codes: "Eenheid.csv",
      suppliers: "Leverancier.csv"
    }

    @output_files = {
      public: "product-catalog-codelists.ttl",
      private: "product-catalog-sensitive.ttl"
    }
  end

  def input_file_path(file)
    "#{@input_dir}/#{@input_files[file]}"
  end

  def output_file_path(file)
    "#{@output_dir}/#{@output_files[file]}"
  end

  def to_s
    puts "Configuration:"
    puts "-- Input dir: #{@input_dir}"
    puts "-- Output dir: #{@output_dir}"
  end
end

###
# Data conversion
###
config = Configuration.new
public_graph = RDF::Graph.new
graph = RDF::Graph.new

puts "[STARTED] Starting products-data-convertor"
puts config.to_s
puts ""

print "[ONGOING] Generating product categories concept scheme..."
uuid = "24a8fa46-378d-440d-ae26-5c1f9eaa4319"
categories_concept_scheme = RDF::URI(BASE_URI % { resource: "concept-schemes", id: uuid })
public_graph << RDF.Statement(categories_concept_scheme, RDF.type, SKOS.ConceptScheme)
public_graph << RDF.Statement(categories_concept_scheme, MU.uuid, uuid)
public_graph << RDF.Statement(categories_concept_scheme, SKOS.prefLabel, "Product categories concept scheme")
puts " done"

print "[ONGOING] Generating unit codes concept scheme..."
uuid = "553bcca5-b4bc-44f3-9fd0-2c79612b982c"
unit_codes_concept_scheme = RDF::URI(BASE_URI % { resource: "concept-schemes", id: uuid })
public_graph << RDF.Statement(unit_codes_concept_scheme, RDF.type, SKOS.ConceptScheme)
public_graph << RDF.Statement(unit_codes_concept_scheme, MU.uuid, uuid)
public_graph << RDF.Statement(unit_codes_concept_scheme, SKOS.prefLabel, "Unit codes concept scheme")
puts " done"

print "[ONGOING] Generating calculation basis concept scheme..."
uuid = "ea700201-8d6d-4230-bbd4-87d49ed988f1"
calculation_basis_concept_scheme = RDF::URI(BASE_URI % { resource: "concept-schemes", id: uuid })
public_graph << RDF.Statement(calculation_basis_concept_scheme, RDF.type, SKOS.ConceptScheme)
public_graph << RDF.Statement(calculation_basis_concept_scheme, MU.uuid, uuid)
public_graph << RDF.Statement(calculation_basis_concept_scheme, SKOS.prefLabel, "Calculation basis concept scheme")
puts " done"

print "[ONGOING] Generating calculation basis concepts..."
uuid = "47c2570e-cc21-4496-ba76-7e89a3cf782d"
vkp_calculation_basis = RDF::URI(BASE_URI % { resource: "calculation-basis", id: uuid })
public_graph << RDF.Statement(vkp_calculation_basis, RDF.type, SKOS.Concept)
public_graph << RDF.Statement(vkp_calculation_basis, RDF.type, EXT["CalculationBasis"])
public_graph << RDF.Statement(vkp_calculation_basis, MU.uuid, uuid)
public_graph << RDF.Statement(vkp_calculation_basis, SKOS.prefLabel, "VKP")
public_graph << RDF.Statement(vkp_calculation_basis, SKOS.inScheme, calculation_basis_concept_scheme)
public_graph << RDF.Statement(vkp_calculation_basis, SKOS.topConceptOf, calculation_basis_concept_scheme)

uuid = "12c12fe4-9d88-4a63-9223-2c83d69da729"
margin_calculation_basis = RDF::URI(BASE_URI % { resource: "calculation-basis", id: uuid })
public_graph << RDF.Statement(margin_calculation_basis, RDF.type, SKOS.Concept)
public_graph << RDF.Statement(margin_calculation_basis, RDF.type, EXT["CalculationBasis"])
public_graph << RDF.Statement(margin_calculation_basis, MU.uuid, uuid)
public_graph << RDF.Statement(margin_calculation_basis, SKOS.prefLabel, "Marge")
public_graph << RDF.Statement(margin_calculation_basis, SKOS.inScheme, calculation_basis_concept_scheme)
public_graph << RDF.Statement(margin_calculation_basis, SKOS.topConceptOf, calculation_basis_concept_scheme)
puts " done"

print "[ONGOING] Generating business categories concept scheme..."
uuid = "75ee1704-e2c7-4401-9b56-f57dfdc36064"
business_categories_concept_scheme = RDF::URI(BASE_URI % { resource: "concept-schemes", id: uuid })
public_graph << RDF.Statement(business_categories_concept_scheme, RDF.type, SKOS.ConceptScheme)
public_graph << RDF.Statement(business_categories_concept_scheme, MU.uuid, uuid)
public_graph << RDF.Statement(business_categories_concept_scheme, SKOS.prefLabel, "Business categories concept scheme")
puts " done"

print "[ONGOING] Generating business categories concepts..."
uuid = "34104acc-3633-4ea6-a465-87075d0e1cd1"
supplier_business_category = RDF::URI(BASE_URI % { resource: "business-categories", id: uuid })
public_graph << RDF.Statement(supplier_business_category, RDF.type, SKOS.Concept)
public_graph << RDF.Statement(supplier_business_category, RDF.type, EXT["BusinessCategory"])
public_graph << RDF.Statement(supplier_business_category, MU.uuid, uuid)
public_graph << RDF.Statement(supplier_business_category, SKOS.prefLabel, "Supplier")
public_graph << RDF.Statement(supplier_business_category, SKOS.inScheme, business_categories_concept_scheme)
public_graph << RDF.Statement(supplier_business_category, SKOS.topConceptOf, business_categories_concept_scheme)
puts " done"

print "[ONGOING] Generating business entity Rollvolet..."
uuid = "b5e1f237-6b17-4698-b581-e0a61396936f"
rollvolet = RDF::URI(BASE_URI % { resource: "business-entities", id: uuid })
public_graph << RDF.Statement(rollvolet, RDF.type, GR["BusinessEntity"])
public_graph << RDF.Statement(rollvolet, MU.uuid, uuid)
public_graph << RDF.Statement(rollvolet, GR.name, "Rollvolet")
puts " done"

categories_input_file = config.input_file_path(:categories)
categories_uri_map = {}
print "[ONGOING] Converting categories found in #{categories_input_file}..."
CSV.foreach(categories_input_file, headers: true) do |row|
  uuid = BSON::ObjectId.new.to_s
  subject = RDF::URI(BASE_URI % { resource: "product-categories", id: uuid })
  name = row["Naam"].downcase
  created = DateTime.parse(row["Cre_Timestamp"])
  modified = DateTime.parse(row["Upd_Timestamp"])

  public_graph << RDF.Statement(subject, RDF.type, SKOS.Concept)
  public_graph << RDF.Statement(subject, RDF.type, EXT["ProductCategory"])
  public_graph << RDF.Statement(subject, MU.uuid, uuid)
  public_graph << RDF.Statement(subject, DCT.identifier, row["ID"])
  public_graph << RDF.Statement(subject, SKOS.prefLabel, name)
  public_graph << RDF.Statement(subject, SKOS.inScheme, categories_concept_scheme)
  public_graph << RDF.Statement(subject, SKOS.topConceptOf, categories_concept_scheme)
  public_graph << RDF.Statement(subject, DCT.created, created)
  public_graph << RDF.Statement(subject, DCT.modified, modified)

  categories_uri_map[row["ID"]] = subject
end
puts " done"

subcategories_input_file = config.input_file_path(:subcategories)
subcategories_uri_map = {}
print "[ONGOING] Converting subcategories found in #{subcategories_input_file}..."
CSV.foreach(subcategories_input_file, headers: true) do |row|
  uuid = BSON::ObjectId.new.to_s
  subject = RDF::URI(BASE_URI % { resource: "product-categories", id: uuid })
  name = row["Naam"].downcase
  parent_category = categories_uri_map[row["CategorieID"]]
  puts "Unable to find parent for subcategory #{row["ID"]}" if parent_category.nil?
  created = DateTime.parse(row["Cre_Timestamp"])
  modified = DateTime.parse(row["Upd_Timestamp"])

  public_graph << RDF.Statement(subject, RDF.type, SKOS.Concept)
  public_graph << RDF.Statement(subject, RDF.type, EXT["ProductCategory"])
  public_graph << RDF.Statement(subject, MU.uuid, uuid)
  public_graph << RDF.Statement(subject, DCT.identifier, row["ID"])
  public_graph << RDF.Statement(subject, SKOS.prefLabel, name)
  public_graph << RDF.Statement(subject, SKOS.inScheme, categories_concept_scheme)
  public_graph << RDF.Statement(subject, SKOS.broader, parent_category) unless parent_category.nil?
  public_graph << RDF.Statement(subject, DCT.created, created)
  public_graph << RDF.Statement(subject, DCT.modified, modified)

  subcategories_uri_map[row["ID"]] = subject
end
puts " done"

unit_codes_input_file = config.input_file_path(:unit_codes)
unit_codes_uri_map = {}
print "[ONGOING] Converting unit codes found in #{unit_codes_input_file}..."
CSV.foreach(unit_codes_input_file, headers: true) do |row|
  uuid = BSON::ObjectId.new.to_s
  subject = RDF::URI(BASE_URI % { resource: "unit-codes", id: uuid })
  name = row["Omschrijving"].downcase
  created = DateTime.parse(row["Cre_Timestamp"])
  modified = DateTime.parse(row["Upd_Timestamp"])

  public_graph << RDF.Statement(subject, RDF.type, SKOS.Concept)
  public_graph << RDF.Statement(subject, RDF.type, EXT["UnitCode"])
  public_graph << RDF.Statement(subject, MU.uuid, uuid)
  public_graph << RDF.Statement(subject, DCT.identifier, row["ID"])
  public_graph << RDF.Statement(subject, SKOS.prefLabel, name)
  public_graph << RDF.Statement(subject, SKOS.inScheme, unit_codes_concept_scheme)
  public_graph << RDF.Statement(subject, SKOS.topConceptOf, unit_codes_concept_scheme)
  public_graph << RDF.Statement(subject, DCT.created, created)
  public_graph << RDF.Statement(subject, DCT.modified, modified)

  unit_codes_uri_map[row["ID"]] = subject
end
puts " done"

departments_input_file = config.input_file_path(:departments)
departments_uri_map = {}
print "[ONGOING] Converting warehouse departments found in #{departments_input_file}..."
CSV.foreach(departments_input_file, headers: true, encoding: "utf-8") do |row|
  uuid = BSON::ObjectId.new.to_s
  subject = RDF::URI(BASE_URI % { resource: "warehouse-departments", id: uuid })
  name = row["Omschrijving"].downcase

  public_graph << RDF.Statement(subject, RDF.type, STOCK["WarehouseDepartment"])
  public_graph << RDF.Statement(subject, MU.uuid, uuid)
  public_graph << RDF.Statement(subject, DCT.identifier, row["ID"])
  public_graph << RDF.Statement(subject, SCHEMA.identifier, row["Afdeling"])
  public_graph << RDF.Statement(subject, SCHEMA.name, name)

  departments_uri_map[row["ID"].to_i] = subject
end
puts " done"

suppliers_input_file = config.input_file_path(:suppliers)
suppliers_uri_map = {}
print "[ONGOING] Converting suppliers found in #{suppliers_input_file}..."
CSV.foreach(suppliers_input_file, headers: true) do |row|
  uuid = BSON::ObjectId.new.to_s
  subject = RDF::URI(BASE_URI % { resource: "business-entities", id: uuid })
  name = row["Naam"]
  created = DateTime.parse(row["Cre_Timestamp"])
  modified = DateTime.parse(row["Upd_Timestamp"])

  graph << RDF.Statement(subject, RDF.type, GR["BusinessEntity"])
  graph << RDF.Statement(subject, MU.uuid, uuid)
  graph << RDF.Statement(subject, DCT.identifier, row["ID"])
  graph << RDF.Statement(subject, GR.name, name)
  graph << RDF.Statement(subject, DCT.created, created)
  graph << RDF.Statement(subject, DCT.modified, modified)
  graph << RDF.Statement(subject, GR.category, supplier_business_category)

  suppliers_uri_map[row["ID"].to_i] = subject
end
puts " done"

products_supplier_input_file = config.input_file_path(:product_suppliers)
print "[ONGOING] Reading product-suppliers data found in #{products_supplier_input_file}..."
product_supplier_rows = CSV.read(products_supplier_input_file, headers: true)
puts " done"

products_input_file = config.input_file_path(:products)
products_uri_map = {}
print "[ONGOING] Converting products found in #{products_input_file}..."
CSV.foreach(products_input_file, headers: true, encoding: "utf-8") do |row|
  uuid = BSON::ObjectId.new.to_s
  product = RDF::URI(BASE_URI % { resource: "products", id: uuid })
  created = DateTime.parse(row["Cre_Timestamp"])
  modified = DateTime.parse(row["Upd_Timestamp"])
  category = subcategories_uri_map[row["SubCategorieID"]]

  graph << RDF.Statement(product, RDF.type, GR["SomeItems"])
  graph << RDF.Statement(product, MU.uuid, uuid)
  graph << RDF.Statement(product, DCT.identifier, row["ID"])
  graph << RDF.Statement(product, GR.name, row["Omschrijving"])
  graph << RDF.Statement(product, GR.description, row["Opmerking"]) unless row["Opmerking"].nil?
  graph << RDF.Statement(product, GR.category, category) unless category.nil?
  graph << RDF.Statement(product, DCT.created, created)
  graph << RDF.Statement(product, DCT.modified, modified)

  products_uri_map[row["ID"]] = product

  # Warehouse location
  warehouse_location_uuid = BSON::ObjectId.new.to_s
  warehouse_location = RDF::URI(BASE_URI % { resource: "warehouse-locations", id: warehouse_location_uuid })
  department = departments_uri_map[row["AfdelingId"].to_i] unless row["AfdelingId"].nil?
  puts "No department found with key #{row["AfdelingId"]}" if department.nil?

  graph << RDF.Statement(warehouse_location, RDF.type, STOCK["WarehouseLocation"])
  graph << RDF.Statement(warehouse_location, MU.uuid, warehouse_location_uuid)
  graph << RDF.Statement(product, STOCK.location, warehouse_location)
  graph << RDF.Statement(warehouse_location, STOCK.department, department) unless department.nil?

  # Purchase offer + price spec
  purchase_rows = product_supplier_rows.find_all { |r| r["Art_ID"] == row["ID"] }
  if purchase_rows.count > 0
    puts "- Warning - Multiple product-supplier rows found for product #{row["ID"]}" if purchase_rows.count > 1
    purchase_row = purchase_rows.first

    purchase_offer_uuid = BSON::ObjectId.new.to_s
    purchase_offer = RDF::URI(BASE_URI % { resource: "offerings", id: purchase_offer_uuid })
    purchase_price_uuid = BSON::ObjectId.new.to_s
    purchase_price = RDF::URI(BASE_URI % { resource: "price-specifications", id: purchase_price_uuid })
    purchase_created = DateTime.parse(purchase_row["Cre_Timestamp"])
    purchase_modified = DateTime.parse(purchase_row["Upd_Timestamp"])
    purchase_identifier = purchase_row["LevNummer"].to_s if purchase_row["LevNummer"]
    supplier = suppliers_uri_map[purchase_row["Lev_ID"].to_i] unless purchase_row["Lev_ID"].nil?

    graph << RDF.Statement(purchase_offer, RDF.type, GR["Offering"])
    graph << RDF.Statement(purchase_offer, MU.uuid, purchase_offer_uuid)
    graph << RDF.Statement(purchase_offer, GR.name, "Inkoopprijs")
    graph << RDF.Statement(supplier, GR.offers, purchase_offer) unless supplier.nil?
    graph << RDF.Statement(product, EXT.purchaseOffering, purchase_offer)
    graph << RDF.Statement(purchase_offer, GR.hasPriceSpecification, purchase_price)
    graph << RDF.Statement(purchase_offer, GR.validFrom, purchase_created)
    graph << RDF.Statement(purchase_offer, DCT.identifier, purchase_identifier) unless purchase_identifier.nil?

    graph << RDF.Statement(purchase_price, RDF.type, GR["UnitPriceSpecification"])
    graph << RDF.Statement(purchase_price, MU.uuid, purchase_price_uuid)
    graph << RDF.Statement(purchase_price, GR.hasCurrency, "EUR")
    graph << RDF.Statement(purchase_price, GR.hasCurrencyValue, RDF::Literal.new(purchase_row["IKP"], datatype: RDF::XSD.double))
    graph << RDF.Statement(purchase_price, GR.valueAddedTaxIncluded, RDF::Literal::FALSE)
    graph << RDF.Statement(purchase_price, DCT.created, purchase_created)
    graph << RDF.Statement(purchase_price, DCT.modified, purchase_modified)

    purchase_unit = unit_codes_uri_map[purchase_row["IKPEenheid"]]
    graph << RDF.Statement(purchase_price, GR.hasUnitOfMeasurement, purchase_unit) unless purchase_unit.nil?
  else
    puts "- Warning - No product-supplier row found for product #{row["ID"]}"
  end

  # Selling offer + price spec
  selling_offer_uuid = BSON::ObjectId.new.to_s
  selling_offer = RDF::URI(BASE_URI % { resource: "offerings", id: selling_offer_uuid })
  selling_price_uuid = BSON::ObjectId.new.to_s
  selling_price = RDF::URI(BASE_URI % { resource: "price-specifications", id: selling_price_uuid })
  selling_created = DateTime.parse(row["Cre_Timestamp"])
  selling_modified = DateTime.parse(row["PrijsGewijzigd"])
  calc_basis = if row["VKPBasis"] == "VKP" then vkp_calculation_basis else margin_calculation_basis end

  graph << RDF.Statement(selling_offer, RDF.type, GR["Offering"])
  graph << RDF.Statement(selling_offer, MU.uuid, selling_offer_uuid)
  graph << RDF.Statement(selling_offer, GR.name, "Verkoopprijs")
  graph << RDF.Statement(selling_offer, SCHEMA.availability, SCHEMA["InStock"])
  graph << RDF.Statement(rollvolet, GR.offers, selling_offer)
  graph << RDF.Statement(product, EXT.salesOffering, selling_offer)
  graph << RDF.Statement(selling_offer, GR.hasPriceSpecification, selling_price)
  graph << RDF.Statement(selling_offer, GR.validFrom, selling_created)

  graph << RDF.Statement(selling_price, RDF.type, GR["UnitPriceSpecification"])
  graph << RDF.Statement(selling_price, MU.uuid, selling_price_uuid)
  graph << RDF.Statement(selling_price, GR.hasCurrency, "EUR")
  graph << RDF.Statement(selling_price, GR.hasCurrencyValue, RDF::Literal.new(row["VKPIncl"], datatype: RDF::XSD.double)) unless row["VKPIncl"].nil?
  graph << RDF.Statement(selling_price, GR.valueAddedTaxIncluded, RDF::Literal::TRUE)
  graph << RDF.Statement(selling_price, PRICE.margin, RDF::Literal.new(row["Marge"], datatype: RDF::XSD.double)) unless row["Marge"].nil?
  graph << RDF.Statement(selling_price, PRICE.calculationBasis, calc_basis)
  graph << RDF.Statement(selling_price, DCT.created, selling_created)
  graph << RDF.Statement(selling_price, DCT.modified, selling_modified)

  selling_unit = unit_codes_uri_map[row["VKPEenheid"]]
  graph << RDF.Statement(selling_price, GR.hasUnitOfMeasurement, selling_unit) unless selling_unit.nil?

  if row["Foto"]
    virtual_file_uuid = BSON::ObjectId.new.to_s
    virtual_file = RDF::URI(BASE_URI % { resource: "files", id: virtual_file_uuid })
    i = row["Foto"].rindex("\\")
    file_name = row["Foto"][i+1..-1]
    cleaned_file_name = file_name.gsub(/\s/, "-")
    file_extension = File.extname(file_name)[1..-1].downcase
    mime_type = "image/jpg"
    file_path = "data/input/images/#{file_name}"
    file_size = File.size(file_path)

    graph << RDF.Statement(virtual_file, RDF.type, NFO["FileDataObject"])
    graph << RDF.Statement(virtual_file, MU.uuid, virtual_file_uuid)
    graph << RDF.Statement(virtual_file, NFO.fileName, file_name)
    graph << RDF.Statement(virtual_file, NFO.fileSize, RDF::Literal.new(file_size, datatype: RDF::XSD.integer))
    graph << RDF.Statement(virtual_file, DCT.format, mime_type)
    graph << RDF.Statement(virtual_file, DBPEDIA.fileExtension, file_extension)
    graph << RDF.Statement(virtual_file, DCT.created, created)
    graph << RDF.Statement(product, DCT.hasPart, virtual_file)

    physical_file_uuid = BSON::ObjectId.new.to_s
    physical_file_name = "#{physical_file_uuid}.#{file_extension}"
    physical_file = RDF::URI("share://#{physical_file_name}")
    graph << RDF.Statement(physical_file, RDF.type, NFO["FileDataObject"])
    graph << RDF.Statement(physical_file, MU.uuid, physical_file_uuid)
    graph << RDF.Statement(physical_file, NFO.fileName, physical_file_name)
    graph << RDF.Statement(physical_file, NFO.fileSize, RDF::Literal.new(file_size, datatype: RDF::XSD.integer))
    graph << RDF.Statement(physical_file, DCT.format, mime_type)
    graph << RDF.Statement(physical_file, DBPEDIA.fileExtension, file_extension)
    graph << RDF.Statement(physical_file, DCT.created, created)
    graph << RDF.Statement(physical_file, NIE.dataSource, virtual_file)

    output_file_path = "data/output/images/#{physical_file_name}"
    file_data = File.open(file_path, "r") { |f| f.read }
    File.open(output_file_path, "w") { |f| f.write file_data }
  end
end
puts " done"


print "[ONGOING] Writing generated data to files..."
RDF::Writer.open(config.output_file_path(:public)) { |writer| writer << public_graph }
RDF::Writer.open(config.output_file_path(:private)) { |writer| writer << graph }
puts " done"

puts "[COMPLETED] Products data conversion finished."
