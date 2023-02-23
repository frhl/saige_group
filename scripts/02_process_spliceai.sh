#!/usr/bin/env bash
#
# @description process_spliceai variants using Hail
# @depends quality controlled MatrixTables with variants.
#
#SBATCH --account=lindgren.prj
#SBATCH --job-name=process_spliceai
#SBATCH --chdir=/well/lindgren-ukbb/projects/ukbb-11867/flassen/projects/KO/for_nik
#SBATCH --output=logs/process_spliceai.log
#SBATCH --error=logs/process_spliceai.errors.log
#SBATCH --partition=short
#SBATCH --cpus-per-task 1
#SBATCH --array=23

#$ -N process_spliceai
#$ -wd /well/lindgren-ukbb/projects/ukbb-11867/flassen/projects/KO/for_nik
#$ -o logs/process_spliceai.log
#$ -e logs/process_spliceai.errors.log
#$ -P lindgren.prjc
#$ -pe shmem 1
#$ -q short.qc
#$ -t 22
#$ -V

set -o errexit
set -o nounset

source utils/qsub_utils.sh
source utils/hail_utils.sh

readonly spark_dir="data/tmp/spark"

readonly array_idx=$( get_array_task_id )
readonly chr=$( get_chr ${array_idx} )

readonly spliceai_dir="data/spliceai"
readonly spliceai_path="${spliceai_dir}/spliceai_annos.mt"
readonly spliceai_type="mt"

readonly out_dir="data/spliceai"
readonly out_prefix="${out_dir}/ukb_wes_450k.spliceai.chr${chr}"
readonly hail_script="scripts/02_process_spliceai.py"

mkdir -p ${out_dir}
mkdir -p ${spark_dir}

set +eu
set_up_hail 0.2.97
set_up_pythonpath_legacy  
python3 ${hail_script} \
     --spliceai_path "${spliceai_path}" \
     --out_prefix "${out_prefix}" \
     --chrom "chr${chr}"




