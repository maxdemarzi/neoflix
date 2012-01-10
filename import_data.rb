require 'rubygems'
require 'neography'

@neo = Neography::Rest.new(ENV['NEO4J_URL'] || "http://localhost:7474")

# Load Genres
@neo.create_node_index("genres_index", "fulltext","lucene")

genres = %w[Action Adventure Animation Children's Comedy Crime Documentary Drama Fantasy Film-Noir Horror Musical Mystery Romance Sci-Fi Thriller War Western]

batch_command = []

genres.each_with_index do |g,i| 
  batch_command << [:create_node, {"name" => g}]
  batch_command << [:add_node_to_index, "genres_index", "name", g, "{#{i * 2}}"]
end 

genre_nodes = @neo.batch *batch_command

genre_ids={}
genre_nodes.each do |n| 
  genre_ids[n["body"]["data"]["name"]] = n["location"].split('/').last
end

# Load Occupations
@neo.create_node_index("occupations_index", "fulltext","lucene")

occupations = ["other" ,"academic/educator", "artist", "clerical/admin", "college/grad student",
               "customer service", "doctor/health care", "executive/managerial", "farmer", "homemaker",
               "K-12 student", "lawyer", "programmer", "retired", "sales/marketing", "scientist",
               "self-employed", "technician/engineer", "tradesman/craftsman", "unemployed", "writer"]

batch_command = []

occupations.each_with_index do |g,i| 
  batch_command << [:create_node, {"name" => g}]
  batch_command << [:add_node_to_index, "occupations_index", "name", g, "{#{i * 2}}"]
end 

occupation_nodes = @neo.batch *batch_command

occupation_ids=[]
occupations.each_with_index do |n, i| 
  occupation_ids[i] = occupation_nodes[i * 2]["location"].split('/').last
end

# Load Movies
@neo.create_node_index("movies_index", "fulltext","lucene")

batch_command = []

movies_file = File.new(File.dirname(__FILE__) +"/data/movies.dat", 'r')
movies = IO.readlines(movies_file)

movie_nodes = []
counter = 0
movies.each do |line| 
  movie = line.force_encoding("ISO-8859-1").split("::")
  batch_command << [:create_node, {"type" => "Movie", "movieId" => movie[0].to_i, "title" => movie[1]}]
  batch_command << [:add_node_to_index, "movies_index", "title", movie[1], "{#{counter}}"]

  movie[2].split("|").each do |g|
    batch_command << [:create_relationship, "hasGenre", "{#{counter}}", genre_ids[g.chomp] ] 
  end
  counter = counter + 2 + movie[2].split("|").size

  if counter > 100
    movie_nodes << @neo.batch(*batch_command)
    counter = 0
    batch_command = []
  end
end

movie_nodes << @neo.batch(*batch_command)
#puts movie_nodes.last.inspect


# Load Users
@neo.create_node_index("users_index")

batch_command = []

users_file = File.new(File.dirname(__FILE__) +"/data/users.dat", 'r')
users = IO.readlines(users_file)

user_nodes = []
counter = 0
[users.first].each do |line| 
  user = line.force_encoding("ISO-8859-1").split("::")
  batch_command << [:create_node, {"type" => "User", "userId" => user[0].to_i, "gender" => user[1], "age" => user[2].to_i}]
  batch_command << [:add_node_to_index, "users_index", "userId", user[0].to_i, "{#{counter}}"]
  batch_command << [:create_relationship, "hasOccupation", "{#{counter}}", occupation_ids[user[3].to_i] ] 
  puts batch_command.inspect
  counter = counter + 3

  if counter > 100
    user_nodes << @neo.batch(*batch_command)
    counter = 0
    batch_command = []
    puts "inserted 100 items to #{user_nodes.size} at #{Time.now} #{user_nodes.last.inspect}"
  end
end

user_nodes << @neo.batch(*batch_command)

puts user_nodes.inspect