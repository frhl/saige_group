#!/usr/bin/env bash


# Get full path to ukb_utils directory
# NOTE: This assumes this script has not been moved from ukb_utils/bash
# From: https://stackoverflow.com/a/246128
_source="${BASH_SOURCE[0]}"
while [ -h "${_source}" ]; do # resolve $_source until the file is no longer a symlink
  _dir="$( cd -P "$( dirname "${_source}" )" >/dev/null 2>&1 && pwd )"
  _source="$(readlink "${_source}")"
  [[ ${_source} != /* ]] && _source="$_dir/$SOURCE" # if $_source was a relative symlink, we need to resolve it relative to the path where the symlink file was located
done
ukb_utils_dir="$( cd -P "$( dirname "$_source" )" >/dev/null 2>&1 && cd ../ && pwd )"


# Needed for raise_error
source "${ukb_utils_dir}/utils/qsub_utils.sh"

add_module_to_pythonpath() {
  #
  # Adds Python modules to PYTHONPATH
  # For example:
  #   add_module_to_pythonpath ukb_utils ukb_wes_qc
  # will add the ukb_utils and ukb_wes_qc modules to PYTHONPATH in that order
  #
  for module in "$@"; do
    case "${module}" in
      "ukb_utils")
        export PYTHONPATH="${PYTHONPATH-}:${ukb_utils_dir}/python"
        ;;
      "ukb_wes_qc")
        export PYTHONPATH="${PYTHONPATH}:/well/lindgren/UKBIOBANK/nbaya/wes_200k/ukb_wes_qc/python"
        ;;
      "phase_ukb_wes")
        export PYTHONPATH="${PYTHONPATH}:/well/lindgren/UKBIOBANK/nbaya/wes_200k/phase_ukb_wes/python"
        ;;
      "ukb_prs_pipeline")
        export PYTHONPATH="$PYTHONPATH:/well/lindgren/UKBIOBANK/nbaya/ukb_prs_pipeline"
        ;;
      "ko_utils")
        export PYTHONPATH="${PYTHONPATH}:/well/lindgren/UKBIOBANK/flassen/projects/KO/wes_ko_ukbb/utils/modules/python"
        ;;
      "phase_ukb_imputed")
        export PYTHONPATH="${PYTHONPATH}:/well/lindgren/UKBIOBANK/nbaya/phase_ukb_imputed/python"
        ;;
      *)
        echo "Warning: '${module}' is not a valid module"
    esac
  done
}

# get current cluster
get_current_cluster_hail() {
  if [ ! -z "${SGE_ACCOUNT:-}" ]; then
    echo "sge"
  elif [ ! -z "${SLURM_JOB_ID:-}" ]; then
    echo "slurm"
  else
    raise_error "Could not find SGE/SLURM variables!"
  fi
}

# outdated now that BMRC has changed to SLURM
get_hail_memory() {
  local _cluster=$( get_current_cluster_hail )
  if [ "${_cluster}" == "sge" ]; then
    echo $( get_hail_memory_sge )
  elif [ "${_cluster}" == "slurm" ]; then
    echo $( get_hail_memory_slurm )
  else
    raise_error "get_hail_memory: could not establish if slurm/sge!"
  fi
}


get_hail_memory_sge() {
  if [[ -z ${QUEUE} || -z ${NSLOTS} ]]; then
    raise_error "QUEUE and NSLOTS must both be defined"
  fi
  if [[ "${QUEUE}" = *".qe" || "${QUEUE}" = *".qc" || "${QUEUE}" = *".qa" ]]; then
    local _mem_per_slot=15
  elif [[ "${QUEUE}" = *".qf" ]]; then
    local _mem_per_slot=3
  else
    raise_error "QUEUE must end in either \".qe\", \".qc\", \".qa\" or \".qf\""
  fi
  echo $(( ${_mem_per_slot}*${NSLOTS} ))
}

get_hail_memory_slurm() {
  
  # we assume standard of 15.2 GB memory per slot, see SLURM_MEM_PER_CPU
  if [[ -z ${SLURM_JOB_PARTITION} || -z ${SLURM_CPUS_PER_TASK} ]]; then
    raise_error "QUEUE and NSLOTS must both be defined"
  fi
  if [[ "${SLURM_JOB_PARTITION}" = "short" || "${SLURM_JOB_PARTITION}" = "long" || "${SLURM_JOB_PARTITION}" = "test" ]]; then
    local _mem_per_slot=15
  else
    raise_error "QUEUE must be either 'short' or 'long'."
  fi
  local _final_mem=$(( ${_mem_per_slot} * ${SLURM_CPUS_PER_TASK} ))
  echo ${_final_mem}
}


set_up_conda() {
  # The content of this function is copied from the contents automatically
  # created by conda in my ~/.bashrc file.
  export PS1=${PS1-} # To avoid "PS1: unbound variable" error
  local __conda_setup="$('/apps/eb/skylake/software/Anaconda3/2020.07/bin/conda' 'shell.bash' 'hook' 2> /dev/null)"
  if [ $? -eq 0 ]; then
    eval "$__conda_setup"
  else
    if [ -f "/apps/eb/skylake/software/Anaconda3/2020.07/etc/profile.d/conda.sh" ]; then
      . "/apps/eb/skylake/software/Anaconda3/2020.07/etc/profile.d/conda.sh"
    else
      export PATH="/apps/eb/skylake/software/Anaconda3/2020.07/bin:$PATH"
    fi
  fi
  unset __conda_setup
}

set_up_hail() {

  #local hversion="undef"
  local hversion="${1:-0.2.102}"
  #local hversion="0.2.102"
  if [ -z "${spark_dir-}" ]; then
    spark_dir='/well/lindgren/flassen/spark/default'
    echo "Note: spark_dir has not been set. Defaulting to ${spark_dir}."
  #else
  #  warn_spark_dir_size
  fi
  mkdir -p ${spark_dir} # directory for Hail's temporary Spark output files
  module load Anaconda3/2020.07
  module load java/1.8.0_latest

  set_up_conda
  #source "/apps/eb/skylake/software/Anaconda3/2020.07/etc/profile.d/conda.sh"
  #conda activate /well/lindgren/users/mmq446/conda/skylake/envs/hail-updated
  
  conda activate /well/lindgren/users/mmq446/conda/skylake/envs/hail-v${hversion}
  #module load OpenBLAS/0.3.8-GCC-9.2.0 # required for linear regression
  #export LD_PRELOAD=/apps/eb/skylake/software/OpenBLAS/0.3.8-GCC-9.2.0/lib/libopenblas.so # required for linear regression
  
  local _mem=$( get_hail_memory )
  if [ ! -z ${_mem} ]; then
    export PYSPARK_SUBMIT_ARGS="--conf spark.local.dir=${spark_dir} --conf spark.executor.heartbeatInterval=1000000 --conf spark.network.timeout=1000000  --driver-memory ${_mem}g --executor-memory ${_mem}g pyspark-shell"
  fi
  echo "Note: Using Hail (v${hversion}) with ${_mem}gb of memory"
}

set_up_pythonpath_legacy() {

  if [ -z "${PYTHONPATH-}" ]; then
    PYTHONPATH="" # If PYTHONPATH is unset
  fi

  export PYTHONPATH="${PYTHONPATH}:/well/lindgren/flassen/ressources/ukb"
  export PYTHONPATH="${PYTHONPATH}:/well/lindgren/flassen/ressources/ukb/ukb_common"
  export PYTHONPATH="${PYTHONPATH}:/well/lindgren/flassen/ressources/ukb/ukb_common/src"
  export PYTHONPATH="${PYTHONPATH}:/well/lindgren/flassen/ressources/ukb/ukb_common/src/ukb_common"
  export PYTHONPATH="${PYTHONPATH}:/well/lindgren/flassen/ressources/ukb/ukb_utils/python"
  export PYTHONPATH="${PYTHONPATH}:/well/lindgren/UKBIOBANK/flassen/projects/KO/wes_ko_ukbb/utils/modules/python"
}


# initialize a semaphore with a given number of tokens
# taken from: https://unix.stackexchange.com/questions/103920/parallelize-a-bash-for-loop
open_sem(){
    mkfifo pipe-$$
    exec 3<>pipe-$$
    rm pipe-$$
    local i=$1
    for((;i>0;i--)); do
        printf %s 000 >&3
    done
}

# run the given command asynchronously and pop/push tokens
# taken from: https://unix.stackexchange.com/questions/103920/parallelize-a-bash-for-loop
run_with_lock(){
    local x
    # this read waits until there is something to read
    read -u 3 -n 3 x && ((0==x)) || exit $x
    (
     ( "$@"; )
    printf '%.3d' $? >&3
    )&
}

# check that we have VCFs created for every partition
check_file_count() {
  local prefix=$1 # should be other form "/path/to/output/chr19_header_per_shard/part-*vcf.gz"
  local obs_ct=$( ls -1 ${prefix} | wc -l )
  if ! [[ ${exp_ct} -eq ${obs_ct} ]]; then
    raise_error "exp_ct=${exp_ct} but obs_ct=${obs_ct} (should be equal). Check VCFs matching ${prefix}*"
  fi
}


#set_up_vep105() {
#  module load EnsEMBLCoreAPI/96.0-r20190601-foss-2019a-Perl-5.28.1
#  module load HTSlib/1.9-GCC-8.2.0-2.31.1
#  export PATH="/well/lindgren/flassen/software/VEP/vep105/cache:$PATH"
#  export PATH="/well/lindgren/flassen/software/VEP/vep105/ensembl-vep:$PATH"
#  export PERL5LIB="/well/lindgren/flassen/software/VEP/vep105:$PERL5LIB"
#  #export PERL5LIB="/apps/eb/skylake/software/EnsEMBLCoreAPI/96.0-r20190601-foss-2019a-Perl-5.28.1/lib/perl5/site_perl/5.28.1/x86_64-linux-thread-multi:$PERL5LIB"  
#}

set_up_vep109() {
  module load EnsEMBLCoreAPI/96.0-r20190601-foss-2019a-Perl-5.28.1
  module load HTSlib/1.9-GCC-8.2.0-2.31.1
  export PERL5LIB="/well/lindgren/barney/.vep:$PERL5LIB"
  export PERL5LIB="/well/lindgren/barney/VEP/modules:$PERL5LIB"
  export PERL5LIB="/well/lindgren/barney/VEP:$PERL5LIB"
  export PATH="/well/lindgren/barney/.vep/htslib:$PATH"
}


set_up_vep107() {
  module load VEP/107-GCC-11.3.0
  #module load samtools/1.8-gcc5.4.0 # required for LOFTEE 
  #export PERL5LIB=$PERL5LIB:/well/lindgren/flassen/software/VEP/plugins_grch38/
}

set_up_vep105() {
  module load EnsEMBLCoreAPI/96.0-r20190601-foss-2019a-Perl-5.28.1
  module load HTSlib/1.9-GCC-8.2.0-2.31.1
  export PERL5LIB="/well/lindgren/flassen/software/VEP/vep105:$PERL5LIB"
  export PERL5LIB="/apps/eb/skylake/software/EnsEMBLCoreAPI/96.0-r20190601-foss-2019a-Perl-5.28.1/lib/perl5/site_perl/5.28.1/x86_64-linux-thread-multi:$PERL5LIB"
}


#set_up_vep105() {
#  >&2 echo "Setting up VEP 105"
#  module load GCC/11.3.0
#  module load Perl/5.34.1-GCCcore-11.3.0
#  module load Archive-Zip/1.68-GCCcore-11.3.0
#  module load DBD-mysql/4.050-GCC-11.3.0
#  module load BioPerl/1.7.8-GCCcore-11.3.0
#  module load Bio-DB-HTS/3.01-GCC-11.3.0
#  module load Compress-Raw-Zlib/2.202-GCCcore-11.3.0 
#  #module load EnsEMBLCoreAPI/96.0-r20190601-foss-2019a-Perl-5.28.1
#  export PERL5LIB="/well/lindgren/flassen/software/VEP/vep105/cache:$PERL5LIB"
#  export PATH="/well/lindgren/flassen/software/VEP/vep105/cache:$PATH"
#  export PATH="/well/lindgren/flassen/software/VEP/vep105/ensembl-vep:$PATH"
#}


# setup modules required for running VEP on HAIL using LOFTEE plugin
set_up_vep() {
  module load EnsEMBLCoreAPI/96.0-r20190601-foss-2019a-Perl-5.28.1 # required for LOFTEE
  module load VEP/95.0-foss-2018b-Perl-5.28.0 # required FOR VEP (NOTE: this steps throws some errors since the above module is already loaded. It works nonetheless.)
  module load samtools/1.8-gcc5.4.0 # required for LOFTEE 
  export PERL5LIB=$PERL5LIB:/well/lindgren/flassen/software/VEP/plugins_grch38/
}


# concatenate the per-partition VCFs ("shards") created by Hail's export_vcf(parallel="header_per_shard")
concat_shards() {
  local vcf_prefix_to_concat=$1
  local start_time=${SECONDS}
  local shard_dir="${vcf_prefix_to_concat}_header_per_shard.bgz" # directory containing VCFs representing MatrixTable partitions (or "shards")
  local concatenated_vcf="${vcf_prefix_to_concat}.vcf.bgz"
  local max_proc=$(( ${NSLOTS}-1 )) # maximum allowed number of concurrent processes

  open_sem ${max_proc} # initialize semaphore for multithreaded tabix

  # multithreaded tabix of shards, restricting the number of concurrent tasks
  for f in $( ls -1 ${shard_dir}/part-*{[0-9].bgz} ); do
    run_with_lock make_tabix $f > /dev/null
  done
  wait

  bcftools concat \
    --file-list <( ls -1 ${shard_dir}/part-*[0-9].bgz ) \
    --naive \
    --output-type z \
    --threads $(( ${NSLOTS}-1 )) \
    --output ${out} \
  && vcf_check ${out} \
  && local duration=$(( ${SECONDS}-${start_time} )) \
  && print_update "Finished shard tabix and bcftools concat, out: ${out}" "${duration}" \
  && rm -r ${shard_dir} \
  && print_update "Removed ${shard_dir}/" \
  || raise_error $( print_update "bcftools concat failed for ${shard_dir}")
}

warn_spark_dir_size() {
  local target=${spark_dir}
  local limit=250
  local size=$(du -bs ${target} | awk '{print $1/2^30}')
  if (( $(echo "$size > $limit" |bc -l) )); then
    >&2 echo "WARNING: ${target} is strictly larger than ${limit} GB"
    echo "WARNING: ${target} is strictly larger than ${limit} GB" >> "spark.warning"
  fi
}





