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

raise_error() {
  # Prints error message to stderr
  >&2 echo -e "Error: $1. Exiting." && exit 1
}

get_chr() {
  if [ $1 -eq 23 ]; then
    echo "X"
  elif [ $1 -eq 24 ]; then
    echo "Y"
  elif [ $1 -ge 1 ] && [ $1 -le 22 ]; then
    echo $1
  else 
    raise_error "Chromosome number must be between 1 and 24"
  fi
}

elapsed_time() {
  if [ ! -z ${1-} ]; then 
    echo "Elapsed time: $( echo "scale=2; $1/3600" | bc -l ) hrs "
  fi
}

print_update() {
  local _message=${1-}
  local _duration=${2-}
  local _cluster=$( get_current_cluster )
  if [ "${_cluster}" == "slurm" ]; then
    echo "${_message}. $( elapsed_time ${_duration} )(job id: ${SLURM_JOB_ID}.${SLURM_ARRAY_TASK_ID}, ${SLURM_CPUS_PER_TASK}x${SLURM_JOB_PARTITION}, $( date ))"
  elif [ "${_cluster}" == "sge" ]; then
    echo "${_message}. $( elapsed_time ${_duration} )(job id: ${SGE_TASK_ID}, ${NSLOTS}x${QUEUE}, $( date ))"
  fi

}

source_ukb_utils_scripts() {
  #
  # Source multiple ukb_utils Bash scripts in a single line
  # Example:
  #   source_ukb_utils hail vcf
  # will source bash/hail_utils.sh and bash/vcf_utils.sh
  #
  for script in "$@"; do
    source "${ukb_utils_dir}/bash/${script}_utils.sh"
  done
}

get_ukb_utils_dir() {
  echo ${ukb_utils_dir}
}

print_cluster_stats() {
  local jobs_running=$( qstat -s r | tail -n+3 | wc -l )
  local total_slots=$( qstat -s r | tail -n+3 | awk '{ sum+=$9 }END{print sum}' )
  echo "jobs running: ${jobs_running}"
  echo "total slots in use: ${total_slots}"
}

get_current_cluster() {
  if [ ! -z "${SGE_ACCOUNT:-}" ]; then
    echo "sge"
  elif [ ! -z "${SLURM_JOB_ID:-}" ]; then
    echo "slurm"
  else
    raise_error "Could not find SGE/SLURM variables!"
  fi
}

get_threads() {
  local _threads=1
  local _slots=$(get_slots_vcf)
  local _new_threads=$((${_slots}-1)) 
  if [ "${_new_threads}" -gt "${_threads}" ]; then
    local _threads=${_new_threads}
  fi
  echo "${_threads}"
}

get_array_task_id() {
  local _cluster=$( get_current_cluster )
  if [ "${_cluster}" == "sge" ]; then
    echo "${SGE_TASK_ID}"
  elif [ "${_cluster}" == "slurm" ]; then
    echo "${SLURM_ARRAY_TASK_ID}"
  else
    raise_error "${_cluster} is not a valid cluster!"
  fi
}


get_slots() {
  local _cluster=$( get_current_cluster )
  if [ "${_cluster}" == "sge" ]; then
    echo "${NSLOTS}"
  elif [ "${_cluster}" == "slurm" ]; then
    echo "${SLURM_CPUS_ON_NODE}"
  else
    raise_error "${_cluster} is not a valid cluster!"
  fi
}




