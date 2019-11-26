#!/usr/bin/python
# -*- coding: iso-8859-1 -*-

import os, sys
import argparse
import re
import subprocess
import pymongo
from pymongo import MongoClient
import datetime


def get_parser():


	parser = argparse.ArgumentParser(description='sequencing technology finder in GAMeRdb')
	
	parser.add_argument("-nas", action="store", dest="nas", type=str, default="/global/bio", help="NAS path (DEFAULT:/global/bio)")
	
	parser.add_argument('-Mu', action="store", dest='MongoUser', type=str, required=True, help='MongoDb username (REQUIRED)')

	parser.add_argument('-Mp', action="store", dest='MongoPassword', type=str, required=True, help='MongoDb password (REQUIRED)')

	return parser


   
def getSampleList(MongoUser, MongoPassword, nas) :
	
	uri = "mongodb://" + MongoUser + ":" + MongoPassword + "@sas-vp-lsdb1/GAMeRdb"
	client = MongoClient(uri)

	db = client.GAMeRdb
	genomes = db.GENOME

	infos = genomes.find()
	for info in infos :
	
		genesCollection = genomes.find({"SampleID":info['SampleID']},{"ProcessingDate":1,"_id":0})
	
		for element in genesCollection :
			if element == {} :
				id = info['SampleID']
				print(id)
				report_path = nas + '/data/GAMeR_DB/' + info['Phylogeny']['Genus'].upper() + '/' + info['SampleID'] + '/' + info['SampleID'] + '_report.txt'

				date = dateParsing(report_path)
				insertMongo(MongoUser, MongoPassword, id, date)
		
		
def insertMongo(MongoUser, MongoPwd, strainid, date):
    
    uri = "mongodb://" + MongoUser + ":" + MongoPwd + "@sas-vp-lsdb1/GAMeRdb"
    client = MongoClient(uri)

    db = client.GAMeRdb
    genomes = db.GENOME

    genomes.update({"SampleID":strainid}, {'$set':{'ProcessingDate':date}})


def dateParsing(reportFile):

	report = open(reportFile,'r')
	line = report.readlines()[1].rstrip()
	
	date = line.split(' ')[1]
	heure = line.split(' ')[-1]

	date = date.split('/')
	new_date = date[2] + '-' + date[1] + '-' + date[0] + 'T' + heure + '.000Z'
	print(new_date)
	d = datetime.datetime.strptime(new_date, "%Y-%m-%dT%H:%M:%S.000Z")
	
	return d
	

def main():


	parser=get_parser()

	#print parser.help if no arguments
	if len(sys.argv)==1:
		parser.print_help()
		sys.exit(1)

	# mettre tout les arguments dans la variable Argument
	Arguments=parser.parse_args()
	
	if Arguments.nas[-1] == '/':
		Arguments.nas = Arguments.nas[:-1]
	
	
	getSampleList(Arguments.MongoUser, Arguments.MongoPassword, Arguments.nas)
	
	
	
	
	
	
if __name__ == "__main__":
		main()
					
