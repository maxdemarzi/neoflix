require 'neography/tasks'
require './neoflix.rb'

namespace :neo4j do
  task :create do
    neo = Neography::Rest.new(ENV['NEO4J_URL'] || "http://localhost:7474")
    create_graph(neo)
  end
end
