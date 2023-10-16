#!/bin/bash

#SBATCH --partition=g100_usr_prod       
# g100_all_serial (default)
# g100_usr_* ; * = | prod | smem | pmem | bmem | interactive |

#SBATCH --qos=g100_qos_dbg
# g100_all_serial = | noQOS | qos_install |
# g100_usr_* = | noQOS | g100_qos_dbg | g100_qos_bprod | g100_qos_lprod |

#SBATCH --account=cin_staff
# | cin_staff | cin_ind |

#SBATCH --mail-type=END,FAIL  # | NONE | BEGIN | END | FAIL | REQUEUE | ALL |
##SBATCH --mail-user=l.babetto@cineca.it

#SBATCH --time=00:01:00
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=1
#SBATCH --mem=500MB  # 3TB for fat nodes

# Printing job info
job=$(basename *.slurm .slurm)
cd $SLURM_SUBMIT_DIR

echo ">> Slurm job $SLURM_JOB_ID - $SLURM_JOB_NAME"                       > $SLURM_SUBMIT_DIR/$job.out
echo ">> Submitted from $SLURM_SUBMIT_HOST:$SLURM_SUBMIT_DIR on $(date)" >> $SLURM_SUBMIT_DIR/$job.out
echo ">> HOME directory: $HOME"                                          >> $SLURM_SUBMIT_DIR/$job.out
echo ">> TMPDIR directory: $TMPDIR"                                      >> $SLURM_SUBMIT_DIR/$job.out
echo ">> SCRATCH directory: $SCRATCH"                                    >> $SLURM_SUBMIT_DIR/$job.out
echo ">> WORK directory: $WORK"                                          >> $SLURM_SUBMIT_DIR/$job.out
echo ">> -------------------"                          >> $SLURM_SUBMIT_DIR/$job.out
echo ">> PATH: $PATH"                                  >> $SLURM_SUBMIT_DIR/$job.out
echo ">> LD_LIBRARY_PATH: $LD_LIBRARY_PATH"            >> $SLURM_SUBMIT_DIR/$job.out
echo ">> -------------------"                          >> $SLURM_SUBMIT_DIR/$job.out
echo ">> NODELIST          : $SLURM_JOB_NODELIST"      >> $SLURM_SUBMIT_DIR/$job.out
echo ">> CPUS_ON_NODE      : $SLURM_CPUS_ON_NODE"      >> $SLURM_SUBMIT_DIR/$job.out
echo ">> CPUS_PER_TASK     : $SLURM_CPUS_PER_TASK"     >> $SLURM_SUBMIT_DIR/$job.out
echo ">> JOB_CPUS_PER_NODE : $SLURM_JOB_CPUS_PER_NODE" >> $SLURM_SUBMIT_DIR/$job.out
echo ">> MEM_PER_NODE      : $SLURM_MEM_PER_NODE"      >> $SLURM_SUBMIT_DIR/$job.out 
echo ">> NTASKS            : $SLURM_NTASKS"            >> $SLURM_SUBMIT_DIR/$job.out
echo ">> NTASKS_PER_NODE   : $SLURM_NTASKS_PER_NODE"   >> $SLURM_SUBMIT_DIR/$job.out 
echo ">> NPROCS            : $SLURM_NPROCS"            >> $SLURM_SUBMIT_DIR/$job.out 
echo ">> TASKS_PER_NODE    : $SLURM_TASKS_PER_NODE"    >> $SLURM_SUBMIT_DIR/$job.out 
echo ">> -------------------"                          >> $SLURM_SUBMIT_DIR/$job.out
echo "" >> $SLURM_SUBMIT_DIR/$job.out

# Load modules, modify as needed
#module load autoload intelmpi/oneapi-2022--binary
module load python/3.8.12--intel--2021.4.0
source $HOME/virtualenvs/dev/bin/activate

# START JOB (actual code goes here)

./$job >> $SLURM_SUBMIT_DIR/$job.out 2> $SLURM_SUBMIT_DIR/$job.err

# END JOB (cleaning up)

echo "" >> $SLURM_SUBMIT_DIR/$job.out
echo ">> -------------------"       >> $SLURM_SUBMIT_DIR/$job.out
echo ">> Job terminated on $(date)" >> $SLURM_SUBMIT_DIR/$job.out
