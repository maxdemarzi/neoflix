require 'rubygems'
require 'neography'
require 'sinatra'
require 'haml'
require 'cgi'
require 'ruby-tmdb'

Tmdb.api_key = ENV['TMDB_KEY']
Tmdb.default_language = "en"

neo = Neography::Rest.new(ENV['NEO4J_URL'] || "http://localhost:7474")

def create_graph(neo)
  return if neo.execute_script("g.idx('vertices')[[type:'Movie']].count();").to_i > 0

  if neo.execute_script("g.indices;").empty?  
    neo.execute_script("g.createAutomaticIndex('vertices', Vertex.class, null);") 
    neo.execute_script("AutomaticIndexHelper.reIndexElements(g, g.idx('vertices'), g.V);") if neo.execute_script("g.V.count();").to_i > 0
  end

  begin
    neo.execute_script("g.setMaxBufferSize(1000);

                    'http://neoflix.heroku.com/movies.dat'.toURL().eachLine { def line ->
                       def components = line.split('::');
                       def movieVertex = g.addVertex(['type':'Movie', 'movieId':components[0].toInteger(), 'title':components[1]]);
                       components[2].split('\\\\|').each { def genera ->
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

                     'http://neoflix.heroku.com/users.dat'.toURL().eachLine { def line ->
                       def components = line.split('::');
                       def userVertex = g.addVertex(['type':'User', 'userId':components[0].toInteger(), 'gender':components[1], 'age':components[2].toInteger()]);
                       def occupation = occupations[components[3].toInteger()];
                       def hits = g.idx(Tokens.T.v)[[occupation:occupation]].iterator();
                       def occupationVertex = hits.hasNext() ? hits.next() : g.addVertex(['type':'Occupation', 'occupation':occupation]);
                       g.addEdge(userVertex, occupationVertex, 'hasOccupation');
                     };

                     'http://neoflix.heroku.com/ratings.dat'.toURL().eachLine {def line ->
                       def components = line.split('::');
                       def ratedEdge = g.addEdge(g.idx(Tokens.T.v)[[userId:components[0].toInteger()]].next(), g.idx(T.v)[[movieId:components[1].toInteger()]].next(), 'rated');
                       ratedEdge.setProperty('stars', components[2].toInteger());
                       };

                      g.stopTransaction(TransactionalGraph.Conclusion.SUCCESS);")

  rescue Timeout::Error 
    puts "Creating the graph is going to take some time, watch it on #{ENV['NEO4J_URL'] || "http://localhost:7474"}"
  end
end
	

  def get_poster(data)
    movie = TmdbMovie.find(:title => CGI::escape(data["title"] || ""), :limit => 1)
    if movie.empty?
     "No Movie Poster found"
    else
      "<a href='#{movie.url}' target='_blank'><img src='#{movie.posters.first.url}'><h3>#{movie.tagline}</h3><p>Rating: #{movie.rating} <br/>Rated: #{movie.certification}</p><p>#{movie.overview}</p>"
    end
  end

  def get_name(data)
    case data["type"]
      when "Movie"
        data["title"]
      when "Occupation"
        data["occupation"]
      when "User"
        "#{data["userId"]} Gender: #{data["gender"]} Age: #{data["age"]}"
      when "Genera"
        data["genera"]
    end
  end

  def node_id(node)
    case node
      when Hash
        node["self"].split('/').last
      when String
        node.split('/').last
      else
        node
    end
  end

  def get_recommendations(neo, node_id)
    rec = neo.execute_script("m = [:];
                              x = [] as Set;
                              v = g.v(node_id);

                              v.
                              out('hasGenera').
                              aggregate(x).
                              back(2).
                              inE('rated').
                              filter{it.getProperty('stars') > 3}.
                              outV.
                              outE('rated').
                              filter{it.getProperty('stars') > 3}.
                              inV.
                              filter{it != v}.
                              filter{it.out('hasGenera').toSet().equals(x)}.
                              groupCount(m){\"${it.id}:${it.title.replaceAll(',',' ')}\"}.iterate();
 
                              m.sort{a,b -> b.value <=> a.value}[0..24];",
                              {:node_id => node_id.to_i})

    return [{"id" => node_id ,"name" => "No Recommendations","values" => [{"id" => "#{node_id}","name" => "No Recommendations"}]}] if rec == "{}"

    values = rec[1..rec.size-1].split(',').collect{ |v| {:id => v.split(':')[0].strip, :name => v.split(':')[1] } }

    [{"id" => node_id ,"name" => "Recommendations","values" => values }]
  end

  get '/resources/show' do
    response.headers['Cache-Control'] = 'public, max-age=2592000'
    content_type :json

    if params[:id].is_numeric?
      node = neo.get_node(params[:id])
    else
      node = neo.execute_script("g.idx(Tokens.T.v)[[title:'#{CGI::unescape(params[:id])}']].next();")
    end

    id = node_id(node)

    {:details_html => "<h2>#{get_name(node["data"])}</h2>" + get_poster(node["data"]),
     :data => {:attributes => get_recommendations(neo, id),
               :name => get_name(node["data"]),
               :id => id}
     }.to_json
  end

  get '/create_graph' do
   # neo.execute_script("g.clear();")
   # create_graph(neo)
  end

  get '/' do
    @neoid = params["movies"]
    haml :index
  end

class String
  def is_numeric?
    Float(self)
    true 
  rescue 
    false
  end
end
