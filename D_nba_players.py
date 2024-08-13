from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Step 1: Load the PySpark SQL Library
spark = SparkSession.builder.appName("NBAPlayersAnalysis").getOrCreate()

# Step 2: Load the dataset into a DataFrame
df = spark.read.csv("all_seasons.csv", header=True)

# Step 3: Transform the DataFrame
# Selecting specific columns: player_name, player_height, player_weight
selected_df = df.select("player_name", "player_height", "player_weight")
selected_df.show(10)

# Filtering based on a condition: selecting players with height above 200cm
filtered_df = df.filter(col("player_height") > 200)
filtered_df.show(10)

# Grouping the data by country and counting the number of players from each country
grouped_df = df.groupBy("country").count()
grouped_df.show()

# Dropping duplicates based on player_name and country columns
deduplicated_df = df.dropDuplicates(["player_name", "country"])
deduplicated_df.show(10)

# Renaming a column: renaming the column "team_abbreviation" to "team_abbr"
renamed_df = df.withColumnRenamed("team_abbreviation", "team_abbr")
renamed_df.show(10)

# Step 4: Reflect on the dataset and questions

# Questions:
# 1. How many total players are there in the dataset and how many of them do not have a college on it's player data?
# 2. Which team has the highest value of average age?
# 3. What is the average height of players drafted in each draft round?

# Step 5: Run SQL queries to answer the questions

# Create a temporary view for SQL queries
df.createOrReplaceTempView("nba_players")

total_players_result = spark.sql("SELECT COUNT(*) AS total_players FROM nba_players").show()

college_none_players_result = spark.sql("SELECT college, COUNT(*) AS player_count FROM nba_players GROUP BY college ORDER BY player_count DESC LIMIT 1").show()

team_highest_avg_age_result = spark.sql("SELECT team_abbreviation, AVG(age) AS avg_age FROM nba_players GROUP BY team_abbreviation ORDER BY avg_age DESC LIMIT 1").show()

question3_result = spark.sql("SELECT draft_round, AVG(player_height) AS avg_height FROM nba_players GROUP BY draft_round").show()



