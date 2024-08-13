from pyspark import SparkContext

# Create a Spark context
sc = SparkContext("local", "NBAPlayersRDD")

# Step 1: Load the dataset into an RDD
nba_rdd = sc.textFile("all_seasons.csv")

# Remove the header line from the RDD
header = nba_rdd.first()
nba_rdd = nba_rdd.filter(lambda line: line != header)

# Step 2: Apply RDD transformations
# All four transformations: map(), filter(), flatMap(), union()

# Example 1: Using map()
player_names_teams_rdd = nba_rdd.map(lambda line: (line.split(",")[1], line.split(",")[2]))

# Example 2: Using filter()
spain_players_35plus_rdd = nba_rdd.filter(lambda line: line.split(",")[7] == "Spain" and float(line.split(",")[3]) > 35)

# Example 3: Using flatMap()
split_names_rdd = nba_rdd.flatMap(lambda line: line.split(",")[1].split(" "))

# Example 4: Using union()
union_rdd = player_names_teams_rdd.union(spain_players_35plus_rdd)

# Step 3: Perform RDD actions
# All four actions: collect(), take(), first(), count()

print("\nPre-RDD Transformation: nba_rdd")
for line in nba_rdd.collect()[:10]:
    print(line)

# 1: collect()
player_names_teams_all = player_names_teams_rdd.collect()
print("\nPlayer Names and Teams (All):\n")
for name, team in player_names_teams_all[:10]:
    print(name, team)

# 2: take()
spain_players_35plus_10 = spain_players_35plus_rdd.take(10)
print("\nSpain Players (Age > 35):\n")
for player in spain_players_35plus_10:
    print(player)

# 3: first()
first_player = nba_rdd.map(lambda line: (line.split(",")[1], line.split(",")[4])).first()
print("\nFirst Player with Height:", first_player)

# 4: count()
union_count = union_rdd.count()
print("\nUnion Count:", union_count)

# Step 4: Group data together
# The sortByKey() and groupByKey() functions

# 1: sortByKey()
sorted_players_by_country = nba_rdd.map(lambda line: (line.split(",")[7], line)).sortByKey()
print("\nSorted Players by Country:\n")
for country, player in sorted_players_by_country.take(10):
    print(country, player)

# 2: groupByKey()
grouped_players_by_country = nba_rdd.map(lambda line: (line.split(",")[7], line)).groupByKey()
print("\nGrouped Players by Country:\n")
for country, players in grouped_players_by_country.take(10):
    print(country)
    count = 0
    for player in players:
        if count < 10:
            print(player)
            count += 1
    print()
