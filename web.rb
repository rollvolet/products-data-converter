require "csv"
require_relative "./products.rb"

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
EXT = RDF::Vocabulary.new("http://mu.semte.ch/vocabularies/ext/")
STOCK = RDF::Vocabulary.new("http://data.rollvolet.be/vocabularies/stock-management/")
PRICE = RDF::Vocabulary.new("http://data.rollvolet.be/vocabularies/pricing/")

BASE_URI = "http://data.rollvolet.be/%{resource}/%{id}"
RESOURCE_BOOLEAN = RDF::URI("http://mu.semte.ch/vocabularies/typed-literals/boolean")

post '/products' do
  start_id = params['start'].to_i
  conversion = ProductConversion.new
  conversion.run start_id
  status 204
end
