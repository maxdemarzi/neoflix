require 'rubygems'
require 'neography'

@neo = Neography::Rest.new(ENV['NEO4J_URL'] || "http://localhost:7474")

# Setup automatic Indexing on all vertices and all properties.   
# null => All keys or it can be a Set<String> of keys to automatically index 

@neo.execute_script("g.createAutomaticIndex('vertices', Vertex.class, null);")

# If the graph already existed prior to creating the AutomaticIndex, 
# then we reIndex all the vertices.

# @neo.execute_script("AutomaticIndexHelper.reIndexElements(g, g.idx('vertices'), g.V);")

@neo.execute_script("g.setMaxBufferSize(1000);
                     occupations = [0:'other', 1:'academic/educator', 2:'artist',
                       3:'clerical/admin', 4:'college/grad student', 5:'customer service',
                       6:'doctor/health care', 7:'executive/managerial', 8:'farmer',
                       9:'homemaker', 10:'K-12 student', 11:'lawyer', 12:'programmer',
                       13:'retired', 14:'sales/marketing', 15:'scientist', 16:'self-employed',
                       17:'technician/engineer', 18:'tradesman/craftsman', 19:'unemployed', 20:'writer'];")

@neo.execute_script("'https://raw.github.com/maxdemarzi/neoflix/master/data/movies.dat'.toURL().eachLine { def line ->
                       def components = line.split('::');
                       def movieVertex = g.addVertex(['type':'Movie', 'movieId':components[0].toInteger(), 'title':components[1]]);
                       components[2].split(/\|/).each { def genera ->
                         def hits = g.idx(Tokens.T.v)[[genera:genera]].iterator();
                         def generaVertex = hits.hasNext() ? hits.next() : g.addVertex(['type':'Genera', 'genera':genera]);
                         g.addEdge(movieVertex, generaVertex, 'hasGenera');
                       }
                     }")

@neo.execute_script("'https://raw.github.com/maxdemarzi/neoflix/master/data/ratings.dat'.toURL().eachLine {def line ->
                       def components = line.split('::');
                       def ratedEdge = g.addEdge(g.idx(Tokens.T.v)[[userId:components[0].toInteger()]].next(), g.idx(T.v)[[movieId:components[1].toInteger()]].next(), 'rated');
                       ratedEdge.setProperty('stars', components[2].toInteger());
                       }")
