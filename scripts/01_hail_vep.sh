#!/usr/bin/env bash
#
# @description annotate variants using Hail
# @depends quality controlled MatrixTables with variants.
#
#SBATCH --account=lindgren.prj
#SBATCH --job-name=hail_vep
#SBATCH --chdir=/well/lindgren/barney/for_nik/
#SBATCH --output=logs/hail_vep.log
#SBATCH --error=logs/hail_vep.errors.log
#SBATCH --partition=short
#SBATCH --cpus-per-task 4
#SBATCH --array=1-23

set -o errexit
set -o nounset

source utils/qsub_utils.sh
source utils/hail_utils.sh

readonly spark_dir="data/tmp/spark"

readonly array_idx=$( get_array_task_id )
readonly chr=$( get_chr ${array_idx} )

readonly in_dir="data/in"
readonly in="${in_dir}/ukb_wes_450k.qced.chr${chr}.ht"

readonly out_dir="data/vep/hail-vep105"
readonly out_prefix="${out_dir}/ukb_wes_450k.qced.chr${chr}"
readonly hail_script="scripts/01_hail_vep.py"

mkdir -p ${out_dir}
mkdir -p ${spark_dir}

if [ ! -f "${out_prefix}_vep.ht/_SUCCESS" ]; then
  set_up_hail 0.2.97
  set_up_vep109
  set_up_pythonpath_legacy  
  python3 ${hail_script} \
       --input_path "${in}" \
       --out_prefix "${out_prefix}"
else
  >&2 echo "${out_prefix}* already exists."
fi




