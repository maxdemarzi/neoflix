require 'rubygems'
require 'neography'
require 'sinatra'

Neography::Rest.default_options[:timeout] = 9000

neo = Neography::Rest.new(ENV['NEO4J_URL'] || "http://localhost:7474")

def create_graph(neo)
  # do not recreate the graph if it already exists
  return if neo.execute_script("g.idx('vertices')[[type:'Movie']].count();").to_i > 0

  # Setup automatic Indexing on all vertices and all properties.   
  # null => All keys or it can be a Set<String> of keys to automatically index 

  if neo.execute_script("g.indices;").empty?  
    neo.execute_script("g.createAutomaticIndex('vertices', Vertex.class, null);") 

    # If vertices already existed prior to creating the AutomaticIndex, 
    # then we reIndex all the vertices.

    neo.execute_script("AutomaticIndexHelper.reIndexElements(g, g.idx('vertices'), g.V);") if neo.execute_script("g.V.count();").to_i > 0
  end

  begin

  neo.execute_script("g.setMaxBufferSize(1000);

                    'https://raw.github.com/maxdemarzi/neoflix/master/data/movies.dat'.toURL().eachLine { def line ->
                       def components = line.split('::');
                       def movieVertex = g.addVertex(['type':'Movie', 'movieId':components[0].toInteger(), 'title':components[1]]);
                       components[2].split(/\|/).each { def genera ->
                         def hits = g.idx(Tokens.T.v)[[genera:genera]].iterator();
                         def generaVertex = hits.hasNext() ? hits.next() : g.addVertex(['type':'Genera', 'genera':genera]);
                         g.addEdge(movieVertex, generaVertex, 'hasGenera');
                       }
                     };

                     occupations = [0:'other', 1:'academic/educator', 2:'artist',
                       3:'clerical/admin', 4:'college/grad student', 5:'customer service',
                       6:'doctor/health care', 7:'executive/managerial', 8:'farmer',
                       9:'homemaker', 10:'K-12 student', 11:'lawyer', 12:'programmer',
                       13:'retired', 14:'sales/marketing', 15:'scientist', 16:'self-employed',
                       17:'technician/engineer', 18:'tradesman/craftsman', 19:'unemployed', 20:'writer'];

                     'https://raw.github.com/maxdemarzi/neoflix/master/data/users.dat'.toURL().eachLine { def line ->
                       def components = line.split('::');
                       def userVertex = g.addVertex(['type':'User', 'userId':components[0].toInteger(), 'gender':components[1], 'age':components[2].toInteger()]);
                       def occupation = occupations[components[3].toInteger()];
                       def hits = g.idx(Tokens.T.v)[[occupation:occupation]].iterator();
                       def occupationVertex = hits.hasNext() ? hits.next() : g.addVertex(['type':'Occupation', 'occupation':occupation]);
                       g.addEdge(userVertex, occupationVertex, 'hasOccupation');
                     };

                     'https://raw.github.com/maxdemarzi/neoflix/master/data/ratings.dat'.toURL().eachLine {def line ->
                       def components = line.split('::');
                       def ratedEdge = g.addEdge(g.idx(Tokens.T.v)[[userId:components[0].toInteger()]].next(), g.idx(T.v)[[movieId:components[1].toInteger()]].next(), 'rated');
                       ratedEdge.setProperty('stars', components[2].toInteger());
                       }")

  puts "Loaded Data"

  rescue Timeout::Error 
    puts "Creating the graph is going to take some time, watch it on #{ENV['NEO4J_URL'] || "http://localhost:7474"}"
  end
end

create_graph(neo)

get '/recreate_graph' do
  neo.execute_script("g.clear();")
  create_graph(neo)
end

get '/' do
  puts "Indices:     " + neo.execute_script("g.indices;").to_s + " should be [""AUTOMATIC[vertices:Vertex][autoIndexKeys:null]""]"
  puts "Vertices:    " + neo.execute_script("g.V.count();").to_s + " should be 9962"
  puts "Edges:       " + neo.execute_script("g.E.count();").to_s + " should be 1012657"
  puts "Movies:      " + neo.execute_script("g.idx('vertices')[[type:'Movie']].count();").to_s + " should be 3883"
  puts "Genera:      " + neo.execute_script("g.idx('vertices')[[type:'Genera']].count();").to_s + " should be 18"
  puts "Users:       " + neo.execute_script("g.idx('vertices')[[type:'User']].count();").to_s + " should be 6040"
  puts "Occupations: " + neo.execute_script("g.idx('vertices')[[type:'Occupation']].count();").to_s + " should be 21"
  puts "Genera:      " + neo.execute_script("g.idx('vertices')[[type:'Genera']].map();").to_s + " should be 18"
end