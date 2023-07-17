#!/usr/bin/env bash
#
# @description annotate variants using Hail
# @depends quality controlled MatrixTables with variants.
#
#SBATCH --account=lindgren.prj
#SBATCH --job-name=create_group_file
#SBATCH --chdir=/well/lindgren-ukbb/projects/ukbb-11867/flassen/projects/KO/for_nik
#SBATCH --output=logs/create_group_file.log
#SBATCH --error=logs/create_group_file.errors.log
#SBATCH --partition=short
#SBATCH --cpus-per-task 1
#SBATCH --array=1-23

#
#$ -N create_group_file
#$ -wd /well/lindgren-ukbb/projects/ukbb-11867/flassen/projects/KO/for_nik
#$ -o logs/create_group_file.log
#$ -e logs/create_group_file.errors.log
#$ -P lindgren.prjc
#$ -pe shmem 1
#$ -q short.qc
#$ -t 22
#$ -V

set -o errexit
set -o nounset

source utils/bash_utils.sh
source utils/qsub_utils.sh

readonly rscript="scripts/04_create_group_file.R"

readonly array_idx=$( get_array_task_id )
readonly chr=$( get_chr ${array_idx} )

readonly in_dir="data/vep/annotated/v4"
readonly in_path="${in_dir}/ukb_wes_450k.qced.brava.v4.chr${chr}.worst_csq_by_gene_canonical.txt.gz"

readonly out_dir="data/vep/saige_group/v4"
readonly out_saige="${out_dir}/ukb_wes_450k.qced.brava.v4.saige_group.chr${chr}.worst_csq_by_gene_canonical.txt"

mkdir -p ${out_dir}

set_up_rpy
Rscript ${rscript} \
  --input_path "${in_path}" \
  --output_path "${out_saige}" \
  --delimiter " "

gzip ${out_saige}


