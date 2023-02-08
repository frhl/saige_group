#!/usr/bin/env bash
#
# @description annotate variants using Hail
# @depends quality controlled MatrixTables with variants.
#
#SBATCH --account=lindgren.prj
#SBATCH --job-name=annotate
#SBATCH --chdir=/well/lindgren-ukbb/projects/ukbb-11867/flassen/projects/KO/for_nik
#SBATCH --output=logs/annotate.log
#SBATCH --error=logs/annotate.errors.log
#SBATCH --partition=short
#SBATCH --cpus-per-task 1
#SBATCH --array=22

#
#$ -N annotate
#$ -wd /well/lindgren-ukbb/projects/ukbb-11867/flassen/projects/KO/for_nik
#$ -o logs/annotate.log
#$ -e logs/annotate.errors.log
#$ -P lindgren.prjc
#$ -pe shmem 1
#$ -q short.qc
#$ -t 23
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

readonly spliceai_dir="data/spliceai"
readonly spliceai_path="${spliceai_dir}/ukb_wes_450k.qced.sites_only.all.vcf"

readonly out_dir="data/vep/annotated"
readonly out_prefix="${out_dir}/ukb_wes_450k.qced.spliceai.chr${chr}"
readonly hail_script="scripts/02_annotate.py"

mkdir -p ${out_dir}
mkdir -p ${spark_dir}

set_up_hail 0.2.97
set_up_pythonpath_legacy  
python3 ${hail_script} \
     --input_path "${in}" \
     --spliceai_path "${splcieai_path}" \
     --out_prefix "${out_prefix}"




