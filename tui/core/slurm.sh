#!/bin/bash
#SBATCH --partition=g100_usr_prod       
#SBATCH --account=cin_staff
#SBATCH --time=00:01:00
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --mem=375300MB
module load python
source $HOME/virtualenvs/dev/bin/activate
./main.py > output.out 2> error.err