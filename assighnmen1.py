import pandas as pd
from pyspark.sql import SparkSession
import findspark
findspark.init()
findspark.find()



spark = SparkSession.builder.master("local").appName("CsvReader").getOrCreate()
print(spark)

data1 = spark.read.format("json").option("header", "true").load("data.json")
print(data1.head(2))
data1.toPandas().to_csv('data.csv', index=False)
data1.show()
ingredientsData = pd.read_csv("data.csv")
beef1 = ingredientsData[ingredientsData["ingredients"].str.contains("Beef")]
beef2 = ingredientsData[ingredientsData["ingredients"].str.contains("beef")]
finalbeef = [beef1, beef2]

finalbeef1 = pd.concat(finalbeef)
finalbeef1.reset_index(drop=True, inplace=True)
finalbeef1['cookTime'] = finalbeef1['cookTime'].map(lambda x: x.lstrip('PT'))
finalbeef1['prepTime'] = finalbeef1['prepTime'].map(lambda x: x.lstrip('PT'))
finalbeef1
duration = list(finalbeef1["prepTime"])
for i in range(len(duration)):
    if len(duration) != 2:
        if "M" in duration[i]:
            duration[i] = duration[i].strip() + " 0H"
        else:
            duration[i] = "0M " + duration[i]
duration[33] = "30M 1H"
duration_hour = []
duration_minute = []
for i in range(len(duration)):
    duration_minute.append(int(duration[i].split(sep="M")[0]))
    duration_hour.append(int(duration[i].split(sep="H")[0].split()[-1]))
finalbeef1.drop(["prepTime"], axis=1, inplace=True)
finalbeef1
finalbeef1["prep_mins"] = duration_minute
finalbeef1["prep_hour"] = duration_hour
finalbeef1
cook_dur = list(finalbeef1["cookTime"])
for i in range(len(cook_dur)):
    if len(cook_dur) != 2:
        if "H" in cook_dur[i]:
            if "M" not in cook_dur[i]:
                cook_dur[i] = cook_dur[i].strip() + "0M"
        else:
            cook_dur[i] = "0H" + cook_dur[i]

cooking_hour = []
cooking_minute = []
for i in range(len(duration)):
    cooking_hour.append(int(cook_dur[i].split("H")[0]))
    cooking_minute.append(int(cook_dur[i].split("H")[1].split("M")[0]))

finalbeef1
finalbeef1.drop(["cookTime"], axis=1, inplace=True)
finalbeef1["cook_mins"] = cooking_minute
finalbeef1["cook_hrs"] = cooking_hour
a = list(finalbeef1["cook_hrs"])
a = [x * 60 for x in a]
b = list(finalbeef1["cook_mins"])
len(a)
c = []
for i in range(len(a)):
    d = a[i] + b[i]
    c.append(d)
finalbeef1["cook_Time"] = c
finalbeef1.drop(["cook_hrs", "cook_mins"], axis=1, inplace=True)
finalbeef1
e = list(finalbeef1["prep_hour"])
e = [x * 60 for x in e]
f = list(finalbeef1["prep_mins"])
g = []
for i in range(len(a)):
    h = e[i] + f[i]
    g.append(h)
finalbeef1["prep_Time"] = g
finalbeef1.drop(["prep_mins", "prep_hour"], axis=1, inplace=True)
ed = list(finalbeef1["recipeYield"])
z = []
totalcookingtime = []
for i in range(len(a)):
    y = int(c[i]) + int(g[i])
    totalcookingtime.append(y)
    th = y / 2
    z.append(th)
finalbeef1
finalbeef1["total_cooking_time"] = totalcookingtime
finalbeef1["avg_total_cooking_time"] = z
finalbeef1
finalbeef1.drop(["url", "image"], axis=1, inplace=True)
finalbeef1
finalbeef1.isnull().values.any()
finalbeef1.to_csv("SortBeef2.csv")
finalbeef1
cList = finalbeef1['avg_total_cooking_time'].tolist()
print(cList)
c = []
for i in cList:
    if i < 30:
        c.append('easy')
    elif i >= 30 and i < 60:
        c.append("medium")
    elif i > 60:
        c.append('hard')
finalbeef1['Difficulty'] = c
Finalreport = finalbeef1.drop(
    ['name', 'ingredients', 'recipeYield', 'datePublished', 'description', 'cook_Time', 'prep_Time',
     'total_cooking_time'], axis=1)
Finalreport.to_csv("secnario2.csv")
