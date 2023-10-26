#!/bin/bash
#SBATCH --partition=g100_usr_prod       
#SBATCH --account=cin_staff
#SBATCH --time=00:01:00
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
module load python
source $HOME/virtualenvs/dtaas/bin/activate
python SCRIPT API_INPUT