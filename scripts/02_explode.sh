#!/usr/bin/env bash
#
# @description annotate variants using Hail
# @depends quality controlled MatrixTables with variants.
#
#SBATCH --account=lindgren.prj
#SBATCH --job-name=explode
#SBATCH --chdir=/well/lindgren-ukbb/projects/ukbb-11867/flassen/projects/KO/for_nik
#SBATCH --output=logs/explode.log
#SBATCH --error=logs/explode.errors.log
#SBATCH --partition=short
#SBATCH --cpus-per-task 1
#SBATCH --array=22

#
#$ -N explode
#$ -wd /well/lindgren-ukbb/projects/ukbb-11867/flassen/projects/KO/for_nik
#$ -o logs/explode.log
#$ -e logs/explode.errors.log
#$ -P lindgren.prjc
#$ -pe shmem 1
#$ -q short.qc
#$ -t 1-22
#$ -V

set -o errexit
set -o nounset

source utils/qsub_utils.sh
source utils/hail_utils.sh

readonly spark_dir="data/tmp/spark"

readonly array_idx=$( get_array_task_id )
readonly chr=$( get_chr ${array_idx} )

readonly in_dir="data/vep/hail"
readonly in="${in_dir}/ukb_wes_450k.qced.chr${chr}.vep.ht"

readonly out_dir="data/vep/explode"
readonly out_prefix="${out_dir}/ukb_wes_450k.qced.chr${chr}"
readonly hail_script="scripts/02_explode.py"

mkdir -p ${out_dir}
mkdir -p ${spark_dir}

set_up_hail 0.2.97
set_up_vep
set_up_pythonpath_legacy  
python3 ${hail_script} \
     --input_path "${in}" \
     --out_prefix "${out_prefix}"




