#
# Assignment5 Interface
# Name: Tanuj Singh
#

from pymongo import MongoClient
import os
import sys
import json
import math

def FindBusinessBasedOnCity(cityToSearch, saveLocation1, collection):
    businessDetails = collection.find({'city': {'$regex':cityToSearch, '$options':"$i"}})
    with open(saveLocation1, "w") as file:
        for business in businessDetails:
            name = business['name']
            full_address = business['full_address'].replace("\n", ", ")
            city = business['city']
            state = business['state']
            file.write(name.upper() + "$" + full_address.upper() + "$" + city.upper() + "$" + state.upper() + "\n")

def FindBusinessBasedOnLocation(categoriesToSearch, myLocation, maxDistance, saveLocation2, collection):
    businessDetails = collection.find({'categories':{'$in': categoriesToSearch}}, {'name': 1, 'latitude': 1, 'longitude': 1, 'categories': 1})
    lat1 = float(myLocation[0])
    lon1 = float(myLocation[1])
    with open(saveLocation2, "w") as file:
        for business in businessDetails:
            name = business['name']
            lat2 = float(business['latitude'])
            lon2 = float(business['longitude'])
            d = DistanceFunction(lat2, lon2, lat1, lon1)
            if d <= maxDistance:
                file.write(name.upper() + "\n")

def DistanceFunction(lat2, lon2, lat1, lon1):
    R = 3959
    pi1 = math.radians(lat1)
    pi2 = math.radians(lat2)
    delta_pi = math.radians(lat2-lat1)
    delta_lambda = math.radians(lon2-lon1)
    a = (math.sin(delta_pi/2) * math.sin(delta_pi/2)) + (math.cos(pi1) * math.cos(pi2) * math.sin(delta_lambda/2) * math.sin(delta_lambda/2))
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
    d = R * c

    return d

