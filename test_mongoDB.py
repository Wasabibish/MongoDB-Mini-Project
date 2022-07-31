from itertools import count
from pymongo import MongoClient
import pandas
from pandas import DataFrame
from requests import request
   
client = MongoClient() 
     
#Connection 
client = MongoClient('localhost', 27017)
print(client)
   
#Base de donnees
database = client['BDD'] 

#Collection    
collection = database['world'] 


#Déterminer le nombre exact de pays
def qst1():
    return len(collection.distinct("Name"))

#Lister les différents continents
def qst2():
    return collection.distinct("Continent")

#Lister les informations de l’Algérie
def qst3():
    return collection.find_one({"Name" : "Algeria"})

#Lister les pays du continent Africain, ayant une population inférieure à 100000 habitants
def qst4():
    countries = []
    request = collection.find({"Continent":"Africa", "Population":{"$lt": 100000}},{"Name" :1})
    for country in (request):
        countries.append(country["Name"])
    return countries



#Lister les pays indépendant du continent océanique
def qst5():
    countries = []
    request = collection.find(
        {"Continent":"Oceania", "IndepYear":{"$ne": "NA"}},
        {"Name" :1, "IndepYear":2 })
    for country in (request):
        countries.append(country["Name"])
    return countries

#Quel est le plus gros continent en termes de surface ?
def qst6():
    request = collection.aggregate([
        {"$group": {"_id": "$Continent", "Surface": {"$sum": "$SurfaceArea"}}},
        {"$sort": {"Surface": -1}},
        {"$limit": 1}])
    countries = []
    for country in (request):
        countries.append(country)
    return countries
def qst7():
    continent = [
        {
            '$project': {
                '_id': 0, 
                'Continent': 1, 
                'Name': 1, 
                'Population': 1, 
                'IndepYear': 1
            }
        }, {
            '$group': {
                '_id': '$Continent', 
                'Pays': {
                    '$sum': 1
                }, 
                'Pop': {
                    '$sum': '$Population'
                
                },
                'inde': {
                    '$sum': {
                        '$cond': [
                            {
                                '$ne': [
                                    '$IndepYear', 'NA'
                                ]
                            }, 1, 0
                        ]
                    }
                }
            }
        }
         ]
    countries = []
    record = collection.aggregate(continent)
    for row in record:
            countries.append(row)
    return countries
    




#Donner la population totale des villes d’Algérie
def qst8():
    request = collection.aggregate([ 
        {"$unwind" :"$Cities" },
        {"$match" :{"Name":"Algeria" }  },
        {"$project":{"Cities.Population" : 1  , "Capital.Population"  : 1  , "_id":0} }  ,
        {"$group": {"_id":"ALGERIA","capital_Pop" :{ "$first": "$Capital.Population" }  , "Population":{"$sum":"$Cities.Population"}}} , 
        {"$project": {"Population_Totale": { "$sum": [ "$capital_Pop"  ,"$Population"  ]  }  } } ])
    
    countries = []
    for country in (request):
        countries.append(country)
    return countries

#Donner la capitale (uniquement nom de la ville et population) d’Algérie    
def qst9():
    request = collection.aggregate([
        {"$match" :{"Name":"Algeria"}  },
        {"$project":{"_id":0, "Capital.Name" : 1, "Capital.Population" : 2     } } ]) 

    countries = []
    for country in (request):
        countries.append(country)
    return countries
#Quelles sont les langues parlées dans plus de 15 pays
def qst10():
    request = collection.aggregate([
        {
            '$unwind': {
                'path': '$OffLang', 
                'preserveNullAndEmptyArrays': False
            }
        }, {
            '$group': {
                '_id': '$OffLang.Language', 
                'Pays': {
                    '$sum': 1
                }
            }
        }, {
            '$match': {
                'Pays': {
                    '$gt': 15
                }
            }
        }
    ])
    countries = []
    for country in request:
        countries.append(country)
       

    return countries


#Calculer pour chaque pays le nombre de villes (pour les pays ayant au moins 100 villes), en les
#triant par ordre décroissant du nombre de villes
def qst11():
    countries = []
    request = collection.aggregate([
        {"$project": 
            { "_id":"$Name",  "nombreDeVilles": 
                { "$cond": { "if": { "$isArray": "$Cities" }, "then": { "$size": "$Cities" }, "else": "0"} } } },
                {  "$match": {"nombreDeVilles": {"$gte":100}  }    }, 
                { "$sort" : { "nombreDeVilles" : -1 } }    ] ) 
    for country in (request):
        countries.append(country)
    return countries

def qst12():
    request = collection.aggregate([ 
    { "$project": { "Name": 1, "Cities.Name": 1, "_id":0}},
    {"$unwind" :"$Cities" },
    { "$sort" : { "Population" : -1 } },
    { "$limit" : 10 }   ])  
    countries = []
    for country in (request):
        countries.append(country)
    return countries
#Lister les pays pour lesquels l’Arabe est une langue officielle
def qst13():
    request = collection.find({ "OffLang.Language" : "Arabic" },{"Name" :1,"_id":0})
    countries = []
    for country in (request):
        countries.append(country)
    return countries 

#Lister les 5 pays avec le plus de langues parlées
def qst14():
    request = collection.aggregate([{"$project":{ 
        "_id":"$Name",
        "nombreDeLangues": { "$sum": [ {      "$size": { "$ifNull": [ "$NotOffLang", [] ] }   }   ,
         {"$size": { "$ifNull": [ "$OffLang", [] ] }   } ]  }  } },
         { "$sort": { "nombreDeLangues": -1 } },
         { "$limit" : 5 }    ] )
    countries = []
    for country in (request):
        countries.append(country)
    return countries 

def qst15():
    request = collection.aggregate([ 
        {"$unwind" :"$Cities" },
        {"$group": {"_id":"$Name" ,"capital_Pop" :{ "$first": "$Capital.Population" }  ,"pays_Population":{ "$first": "$Population" }   , "population":{"$sum":"$Cities.Population"}}} , 
        {"$project": { "Population_Totale": { "$sum": [ "$capital_Pop"  ,"$population"  ]  } ,"pays_Population":"$pays_Population" }  },
        {"$match":{"$expr":{"$gt":["$Population_Totale", "$pays_Population"]}}}   ]) ;

    countries = []
    for country in (request):
        countries.append(country)
    return countries
answer = qst7()
print(answer)













