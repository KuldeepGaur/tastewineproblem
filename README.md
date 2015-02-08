# Taste Wine Problem
## Wine Tasting 
A large group of friends from the town of  Nocillis visit the vineyards of Apan to taste wines. The vineyards produce many fine wines and the friends decide to buy as many as 3 bottles of wine each if they are available to purchase. Unfortunately, the vineyards of Apan have a peculiar restriction that they can not sell more than one bottle of the same wine. So the vineyards come up with the following scheme: They ask each person to write down a list of up to 10 wines that they enjoyed and would be happy buying. With this information, please help the vineyards maximize the number of wines that they can sell to the group of friends.  

## Input  
A two-column TSV file with the first column containing the ID (just a string) of a person and the second column the ID of the wine that they like. Here are three input data sets of increasing sizes. Please send us solutions even if it runs only on the first file.  
#### https://s3.amazonaws.com/br-user/puzzles/person_wine_3.txt 
#### https://s3.amazonaws.com/br-user/puzzles/person_wine_4.txt.zip 
#### https://s3.amazonaws.com/br-user/puzzles/person_wine_5.txt.zip  

## Output  
First line contains the number of wine bottles sold in aggregate with your solution. Each subsequent line should be two columns, tab separated. The first column is an ID of a person and the second column should be the ID of the wine that they will buy.  Please check your work. Note that the IDs of the output second column should be unique since a single bottle of wine can not be sold to two people and an ID on the first column can appear at most three times since each person can only buy up to 3 bottles of wine.

## Approach
I read the input files and store the input data into MongoDB collection. And process the mongoDB data to assign the wines
to the friends who have listed it as their choice.
Algorithm becomes complex because for each M friend and each N wines the runtime will be O(M*N) which is huge for the
data set.
Hence the solution is to assign the wines based on the Hash(wineID)%(numberofwines) and the hashcode will be unique.
Create a bit buffer and set the boolean to true for each wine taken. Any other friend wanting to take same wine will
hash to same bit value and check if its already set then its already taken hence skip and move on. 
This approach will take O(M) which is quite optimized.

## Prerequisites
* MongoDB 2.6.7 server should be installed and running on 27017 port.
* MongoTest client should be helpful in testing the connection.
* Import the project in eclipse and run TastingWine.java. This will create result3.txt file with output in ~/res/.
* The first input file "person_wine_3.txt" is attached with the code.

