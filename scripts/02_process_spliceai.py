#!/usr/bin/env python3

import hail as hl
import argparse

from ukb_utils import hail_init
from ko_utils import io
from ko_utils import ko

def main(args):

    # parser
    input_path = args.input_path
    spliceai_path = args.spliceai_path
    chrom = args.chrom

    hail_init.hail_bmrc_init_local('logs/hail/hail_format.log', 'GRCh38')
    
    # read spliceAI data 
    ht = hl.read_table(spliceai_path)
    ht = ht.rows()
    ht = ht.filter(ht.locus.contig == chrom) 

    # annotate based on this: https://github.com/Illumina/SpliceAI
    ht = ht.annotate(SpliceAI = hl.struct(
        ALLELE = ht.info.SpliceAI[0],
        SYMBOL = ht.info.SpliceAI[1],
        DS_AG = hl.float32(ht.info.SpliceAI[2]),
        DS_AL = hl.float32(ht.info.SpliceAI[3]),
        DS_DG = hl.float32(ht.info.SpliceAI[4]),
        DS_DL = hl.float32(ht.info.SpliceAI[5]),
        DP_AG = hl.int32(ht.info.SpliceAI[6]),
        DP_AL = hl.int32(ht.info.SpliceAI[7]),
        DP_DG = hl.int32(ht.info.SpliceAI[8]),
        DP_DL = hl.int32(ht.info.SpliceAI[9])
    ))

    # drop info and add aggregates
    ht = ht.drop(ht.info)
    ht.write(out_prefix + ".ht")

if __name__=='__main__':
    parser = argparse.ArgumentParser()
    # initial params
    parser.add_argument('--spliceai_path', default=None, help='Path to input')
    parser.add_argument('--chrom', default=None, help='chromosome')
    parser.add_argument('--out_prefix', default=None, help='Path prefix for output dataset')

    args = parser.parse_args()

    main(args)



