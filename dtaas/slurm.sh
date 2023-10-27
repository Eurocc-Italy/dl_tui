#!/bin/bash
#SBATCH --partition=g100_usr_prod       
#SBATCH --account=cin_staff
#SBATCH --time=01:00:00
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=48
module load python
source $HOME/virtualenvs/dtaas/bin/activate
dtaas_wrapper