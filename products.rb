require_relative "./codelists.rb"

###
# I/O Configuration
###
class Configuration
  attr_reader :input_file, :output_file
  def initialize
    @input_dir = "/data/input"
    @output_dir = "/data/output"

    @input_file = "#{@input_dir}/products.csv"
    timestamp = DateTime.now.strftime("%Y%m%d%H%M%S")
    @output_file = "#{@output_dir}/#{timestamp}-products-sensitive.ttl"
  end

  def to_s
    puts "Configuration:"
    puts "-- Input: #{@input_file}"
    puts "-- Output: #{@output_file}"
  end
end

class ProductConversion
  def initialize
    @config = Configuration.new
    @categories_map = Codelists.fetch_categories
    @departments_map = Codelists.fetch_departments
    @unit_codes_map = Codelists.fetch_unit_codes
    @business_entities_map = Codelists.fetch_business_entities
    @calculation_basis_map = Codelists.fetch_calculation_basis
  end

  def run start_id
    puts "[STARTED] Starting products conversion from ID #{start_id}"
    puts @config.to_s
    puts ""

    graph = RDF::Graph.new
    number = start_id
    products_uri_map = {}
    print "[ONGOING] Converting products found in #{@config.input_file}..."
    CSV.foreach(@config.input_file, headers: true, encoding: "utf-8") do |row|
      now = DateTime.now
      uuid = Mu.generate_uuid()
      product = RDF::URI(BASE_URI % { resource: "products", id: uuid })
      subcategory = @categories_map[row["Subcategorie"]]
      puts "No category found with key #{row["Subcategorie"]}" if subcategory.nil?

      graph << RDF.Statement(product, RDF.type, GR["SomeItems"])
      graph << RDF.Statement(product, MU_CORE.uuid, uuid)
      graph << RDF.Statement(product, DCT.identifier, number)
      graph << RDF.Statement(product, GR.name, row["Omschrijving"])
      graph << RDF.Statement(product, GR.category, subcategory) unless subcategory.nil?
      graph << RDF.Statement(product, EXT.includeInStockReport, RDF::Literal.new("true", datatype: RESOURCE_BOOLEAN))
      graph << RDF.Statement(product, DCT.created, now)
      graph << RDF.Statement(product, DCT.modified, now)

      # Warehouse location
      warehouse_location_uuid = Mu.generate_uuid()
      warehouse_location = RDF::URI(BASE_URI % { resource: "warehouse-locations", id: warehouse_location_uuid })
      department = @departments_map[row["Afdeling"]] unless row["Afdeling"].nil?
      puts "No department found with key #{row["Afdeling"]}" if department.nil?

      graph << RDF.Statement(warehouse_location, RDF.type, STOCK["WarehouseLocation"])
      graph << RDF.Statement(warehouse_location, MU_CORE.uuid, warehouse_location_uuid)
      graph << RDF.Statement(product, STOCK.location, warehouse_location)
      graph << RDF.Statement(warehouse_location, STOCK.department, department) unless department.nil?

      # Purchase offer + price spec
      purchase_offer_uuid = Mu.generate_uuid()
      purchase_offer = RDF::URI(BASE_URI % { resource: "offerings", id: purchase_offer_uuid })
      purchase_price_uuid = Mu.generate_uuid()
      purchase_price = RDF::URI(BASE_URI % { resource: "price-specifications", id: purchase_price_uuid })
      supplier = @business_entities_map[row["Leverancier"]] unless row["Leverancier"].nil?

      graph << RDF.Statement(purchase_offer, RDF.type, GR["Offering"])
      graph << RDF.Statement(purchase_offer, MU_CORE.uuid, purchase_offer_uuid)
      graph << RDF.Statement(purchase_offer, GR.name, "Inkoopprijs")
      graph << RDF.Statement(supplier, GR.offers, purchase_offer) unless supplier.nil?
      graph << RDF.Statement(product, EXT.purchaseOffering, purchase_offer)
      graph << RDF.Statement(purchase_offer, GR.hasPriceSpecification, purchase_price)
      graph << RDF.Statement(purchase_offer, GR.validFrom, now)

      graph << RDF.Statement(purchase_price, RDF.type, GR["UnitPriceSpecification"])
      graph << RDF.Statement(purchase_price, MU_CORE.uuid, purchase_price_uuid)
      graph << RDF.Statement(purchase_price, GR.hasCurrency, "EUR")
      graph << RDF.Statement(purchase_price, GR.hasCurrencyValue, RDF::Literal.new(row["IKP"], datatype: RDF::XSD.double))
      graph << RDF.Statement(purchase_price, GR.valueAddedTaxIncluded, RDF::Literal::FALSE)
      graph << RDF.Statement(purchase_price, DCT.created, now)
      graph << RDF.Statement(purchase_price, DCT.modified, now)

      purchase_unit = @unit_codes_map[row["IKPEenheid"].downcase] unless row["IKPEenheid"].nil?
      graph << RDF.Statement(purchase_price, GR.hasUnitOfMeasurement, purchase_unit) unless purchase_unit.nil?

      # Selling offer + price spec
      selling_offer_uuid = Mu.generate_uuid()
      selling_offer = RDF::URI(BASE_URI % { resource: "offerings", id: selling_offer_uuid })
      selling_price_uuid = Mu.generate_uuid()
      selling_price = RDF::URI(BASE_URI % { resource: "price-specifications", id: selling_price_uuid })
      selling_price_vat_in = row["VKPBasis"].to_f * 121/100 unless row["VKPBasis"].nil? # 21% VAT

      graph << RDF.Statement(selling_offer, RDF.type, GR["Offering"])
      graph << RDF.Statement(selling_offer, MU_CORE.uuid, selling_offer_uuid)
      graph << RDF.Statement(selling_offer, GR.name, "Verkoopprijs")
      graph << RDF.Statement(selling_offer, SCHEMA.availability, SCHEMA["InStock"])
      graph << RDF.Statement(@business_entities_map["Rollvolet"], GR.offers, selling_offer)
      graph << RDF.Statement(product, EXT.salesOffering, selling_offer)
      graph << RDF.Statement(selling_offer, GR.hasPriceSpecification, selling_price)
      graph << RDF.Statement(selling_offer, GR.validFrom, now)

      graph << RDF.Statement(selling_price, RDF.type, GR["UnitPriceSpecification"])
      graph << RDF.Statement(selling_price, MU_CORE.uuid, selling_price_uuid)
      graph << RDF.Statement(selling_price, GR.hasCurrency, "EUR")
      graph << RDF.Statement(selling_price, GR.hasCurrencyValue, RDF::Literal.new(selling_price_vat_in, datatype: RDF::XSD.double)) unless selling_price_vat_in.nil?
      graph << RDF.Statement(selling_price, GR.valueAddedTaxIncluded, RDF::Literal.new("true", datatype: RESOURCE_BOOLEAN))
      graph << RDF.Statement(selling_price, PRICE.margin, RDF::Literal.new(row["Marge"], datatype: RDF::XSD.double)) unless row["Marge"].nil?
      graph << RDF.Statement(selling_price, PRICE.calculationBasis, @calculation_basis_map["Margin"])
      graph << RDF.Statement(selling_price, DCT.created, now)
      graph << RDF.Statement(selling_price, DCT.modified, now)

      selling_unit = @unit_codes_map[row["VKPEenheid"].downcase]
      graph << RDF.Statement(selling_price, GR.hasUnitOfMeasurement, selling_unit) unless selling_unit.nil?

      number = number + 1
    end
    puts " done"


    print "[ONGOING] Writing generated data to file..."
    RDF::Writer.open(@config.output_file) { |writer| writer << graph }
    puts " done"

    puts "[COMPLETED] Products data conversion finished."

  end
end

def generate_products(start_id)
end
