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
#SBATCH --array=23

#
#$ -N annotate
#$ -wd /well/lindgren-ukbb/projects/ukbb-11867/flassen/projects/KO/for_nik
#$ -o logs/annotate.log
#$ -e logs/annotate.errors.log
#$ -P lindgren.prjc
#$ -pe shmem 1
#$ -q short.qc
#$ -t 22
#$ -V

#set -o errexit
#set -o nounset

source utils/qsub_utils.sh
source utils/hail_utils.sh

readonly spark_dir="data/tmp/spark"

readonly array_idx=$( get_array_task_id )
readonly chr=$( get_chr ${array_idx} )

readonly vep_dir="/well/lindgren/barney/for_nik/data/vep/hail-vep105"
readonly vep="${vep_dir}/ukb_wes_450k.qced.chr${chr}.vep_with_gnomad.ht"

readonly spliceai_dir="/well/lindgren/barney/for_nik/data/spliceai"
readonly spliceai_path="${spliceai_dir}/ukb_wes_450k.spliceai.chr${chr}.ht"

readonly out_dir="data/vep/annotated/v5-test-chrX"
readonly out_prefix="${out_dir}/ukb_wes_450k.qced.brava.v4.chr${chr}"
readonly hail_script="scripts/03_annotate.py"

mkdir -p ${out_dir}
mkdir -p ${spark_dir}

set +eu
set_up_hail 0.2.97
set_up_pythonpath_legacy  
python3 ${hail_script} \
     --vep_path "${vep}" \
     --spliceai_path "${spliceai_path}" \
     --out_prefix "${out_prefix}"




