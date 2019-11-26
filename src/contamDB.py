#!/usr/bin/python
# -*- coding: iso-8859-1 -*-

import os, sys
import argparse
import re
import subprocess
import pymongo
from pymongo import MongoClient


def get_parser():


    parser = argparse.ArgumentParser(description='contamination finder in GAMeRdb')

    parser.add_argument('-T', action="store", dest='nbThreads', 
                        type=str, default='6', help='Number of threads per task to use (default:6)')
    
    parser.add_argument("-nas", action="store", dest="nas", type=str, default="/global/bio", help="NAS path")
    
    parser.add_argument('-Mu', action="store", dest='MongoUser', type=str, required=True, help='MongoDb username (REQUIRED)')

    parser.add_argument('-Mp', action="store", dest='MongoPassword', type=str, required=True, help='MongoDb password (REQUIRED)')

    return parser



def insertMongo(MongoUser, MongoPwd, strainid, ContamStatus, NumContamSNVs, percentContam):
    
    uri = "mongodb://" + MongoUser + ":" + MongoPwd + "@sas-vp-lsdb1/GAMeRdb"
    client = MongoClient(uri)

    db = client.GAMeRdb
    genomes = db.GENOME

    genomes.update({"SampleID":strainid}, {'$set':{'Contam.ContamStatus':ContamStatus}})
    genomes.update({"SampleID":strainid}, {'$set':{'Contam.NumContamSNVs':NumContamSNVs}})
    genomes.update({"SampleID":strainid}, {'$set':{'Contam.percentContam':percentContam}})
    


def getSampleList(MongoUser, MongoPassword, nas) :
    
    
    uri = "mongodb://" + MongoUser + ":" + MongoPassword + "@sas-vp-lsdb1/GAMeRdb"
    client = MongoClient(uri)

    db = client.GAMeRdb
    genomes = db.GENOME

    infos = genomes.find()
    #infos = genomes.find({"Phylogeny.Genus":"Salmonella"})

    dico_sample = {}

    for info in infos :

        genesCollection = genomes.find({"SampleID":info['SampleID']},{"Contam":1,"_id":0})
        for element in genesCollection :
            if element == {} :
                    path1 = nas + '/data/GAMeR_DB/' + info['Phylogeny']['Genus'].upper() + '/' + info['SampleID'] + '/' + info['SampleID'] + '_R1.fastq.gz'
                    path2 = nas + '/data/GAMeR_DB/' + info['Phylogeny']['Genus'].upper() + '/' + info['SampleID'] + '/' + info['SampleID'] + '_R2.fastq.gz'
                    dico_sample[info['SampleID']] = [path1,path2]


    return(dico_sample)


def slurm_batch(name,cmd,dependencies="",conda_env="confindr",threads='1',sync=0,mem=0 ):

    sbash_file = "confindr_"+name+"_slurm.sh"
    with open(sbash_file,"w") as file:
        file.write("#!/bin/bash\n")
        file.write("#SBATCH -J confindr_"+name+"\n")
        if mem!=0:
            file.write("#SBATCH --mem="+str(mem)+"\n")
        if sync!=0:
            file.write("#SBATCH -W\n")
        if int(threads)>=0:
            file.write("#SBATCH --cpus-per-task="+str(threads)+"\n")
        else:
            file.write("#SBATCH --cpus-per-task=1\n")
        file.write("#SBATCH -o %x.%N.%j.out"+"\n")
        file.write("#SBATCH -e %x.%N.%j.err"+"\n")
        file.write("#SBATCH -p Production\n")
        if conda_env:
            file.write("source /global/conda/bin/activate\nconda activate\nconda activate "+str(conda_env)+"\n\n")
        file.write("\n")
        file.write(str(cmd)+"\n")
        file.flush()
        file.close()
    slurm_cmd=['sbatch ', dependencies, " "+os.getcwd()+"/"+sbash_file]
    os.system("echo "+"".join(slurm_cmd) +" >> " + os.path.splitext(sbash_file)[0] + ".log" )
    print("".join(slurm_cmd))
    #return(re.findall(r'\d+',subprocess.check_output(['ls', '-l'])))
    #return(re.findall(r'\d+',subprocess.check_output("".join(slurm_cmd), shell=True))[0])
    job_return = str(subprocess.check_output("".join(slurm_cmd), shell=True)).rstrip()
    #print(job_return)
    job_return = job_return.split(' ')[-1]
    #print(job_return)
    job_return = job_return.split('\\')[0]
    #print(job_return)
    jobid = job_return
    #return(re.findall(r'\d+',str(subprocess.check_output("".join(slurm_cmd), shell=True)))[0])
    #print("???? " + jobid)
    return(jobid)


def confindrReader(strainid, MongoUser, MongoPwd, nas):
    
    confindr_csv = strainid + "_confindr/confindr_report.csv"
    
    confindr_file = open(confindr_csv,'r')
    lines = confindr_file.readlines()
    confindr_file.close()    
    header = True
    
    for line in lines :
        if header :
            header = False
        else :
            ContamStatus = line.rstrip().split(',')[3]
            NumContamSNVs = line.rstrip().split(',')[2] 
            percentContam = line.rstrip().split(',')[4]
            
    insertMongo(MongoUser, MongoPwd, strainid, ContamStatus, NumContamSNVs, percentContam)


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
    
    
    jobid = []
    to_remove = []
    confindr_outputab = []
    
    
    dico_sample = getSampleList(Arguments.MongoUser, Arguments.MongoPassword, Arguments.nas)
    
    for element in dico_sample :
        
        #print(element)
        
        directory = element + "_reads"
        output = element + "_confindr"
        os.system("mkdir " + directory)
        to_remove.append(directory)
        to_remove.append(output)
        to_remove.append("confindr_" + element + "*")
        os.system("ln -sF " + dico_sample[element][0] + " $PWD/" + directory + "/" + dico_sample[element][0].split('/')[-1])
        os.system("ln -sF " + dico_sample[element][1] + " $PWD/" + directory + "/" + dico_sample[element][1].split('/')[-1])
        cmd = "confindr.py -i " + directory + " -o " + output + " -d /global/bio/data/confindr_db " + " -t " + Arguments.nbThreads + "  -Xmx 8g"
        jobid.append(slurm_batch(element,cmd,"","confindr",Arguments.nbThreads,0,0))  
        confindr_outputab.append(output + '/confindr_report.csv')
        
       
    
    slurm_batch('wait_on','echo finish'," --dependency=afterok:" + ':'.join(jobid),"confindr",1,1,0 )
    to_remove.append("confindr_wait_on*")
    
    #confindrReader(element, Arguments.MongoUser, Arguments.MongoPassword, Arguments.nas)
    
    
    for strainid in dico_sample :
        confindrReader(strainid, Arguments.MongoUser, Arguments.MongoPassword, Arguments.nas)

    os.system('rm -r ' + ' '.join(to_remove))
    
    
    
    
    
    
    
if __name__ == "__main__":
        main()
                    
