#!/bin/bash
#SBATCH -p safe
#SBATCH -J Post_ARTwork                    # fichier où sera écrit la sortie standart STDOUT
#SBATCH -o %x.%N.%j.out            # fichier où sera écrit la sortie standart STDOUT
#SBATCH -e %x.%N.%j.err            # fichier où sera écrit la sortie d'erreur STDERR


source /global/conda/bin/activate
conda activate
conda activate confindr
python contamDB.py -Mu Manager -Mp reganaM

conda activate genial_db
GENIALslurm -Mu Manager -Mp reganaM

conda activate artwork
python ProcessingDate_DB.py -Mu Manager -Mp reganaM
python SeqTechno_FinderDB.py -Mu Manager -Mp reganaM

