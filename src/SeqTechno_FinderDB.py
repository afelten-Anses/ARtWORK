#!/usr/bin/python
# -*- coding: iso-8859-1 -*-

import os, sys
import argparse
import re
import subprocess
import pymongo
from pymongo import MongoClient


def get_parser():


	parser = argparse.ArgumentParser(description='sequencing technology finder in GAMeRdb')
	
	parser.add_argument("-nas", action="store", dest="nas", type=str, default="/global/bio", help="NAS path")
	
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
	
		genesCollection = genomes.find({"SampleID":info['SampleID']},{"Reads.Predicted_Technology":1,"_id":0})
	
		for element in genesCollection :
			if element['Reads'] == {} :
				id = info['SampleID']
				#print(id)
				path1 = nas + '/data/GAMeR_DB/' + info['Phylogeny']['Genus'].upper() + '/' + info['SampleID'] + '/' + info['SampleID'] + '_R1.fastq.gz'
				tmpFile = "SeqTechno_FinderDB.tmp"
				cmd = "/usr/bin/gzip -cd " + path1 + " | head -n1 > " + tmpFile 
				os.system(cmd)

				old_SeqTechno = info["Reads"]["Technology"]
				new_SeqTechno = detect_SequencingTechnology(tmpFile, id, old_SeqTechno)
				os.system("rm " + tmpFile)
				insertMongo(MongoUser, MongoPassword, id, new_SeqTechno)
		
		
def insertMongo(MongoUser, MongoPwd, strainid, new_SeqTechno):
    
    uri = "mongodb://" + MongoUser + ":" + MongoPwd + "@sas-vp-lsdb1/GAMeRdb"
    client = MongoClient(uri)

    db = client.GAMeRdb
    genomes = db.GENOME

    genomes.update({"SampleID":strainid}, {'$set':{'Reads.Predicted_Technology':new_SeqTechno}})


def detect_SequencingTechnology(reads_filepath, id, techno):

	with open(reads_filepath, "r") as f:
		for line in f.readlines():
		
			if line[0]!='@':
				return techno
				
			elif id in line or line[0:4] == "@ERR":
				return techno
			
			elif line[0:6] == "@HWI-M" or line[0:2] == "@M":
				return 'MiSeq'

			elif line[0:6] == "@HWI-C" or line[0:2] == "@C":
				return 'HiSeq 1500'
				
			elif line[0:6] == "@HWUSI" :
				return 'GAIIx'
				
			elif line[0:6] == '@HWI-D' or line[0:2]  == '@D':
				return 'HiSeq 2500'
				
			elif line[0:2]  == '@J':
				return 'HiSeq 3000'
				
			elif line[0:2]  == '@K':
				return 'HiSeq 3000/4000'	
			
			elif line[0:2]  == '@E':
				return 'HiSeq X'	
			
			elif line[0:2] == '@N' :
				return 'NextSeq'
				
			elif line[0:2] == '@A' :
				return 'NovaSeq'

			elif line[0:3] == '@MN' :
				return 'MiniSeq'
			
			else :
				return techno


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
					
