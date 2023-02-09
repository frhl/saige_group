#!/usr/bin/env python3

import hail as hl
import argparse

from ukb_utils import hail_init
from ko_utils import io

def main(args):

    spliceai_path = args.spliceai_path
    out_prefix = args.out_prefix
    chrom = args.chrom

    #
    hail_init.hail_bmrc_init_local('logs/hail/hail_format.log', 'GRCh38')
    ht = hl.read_matrix_table(spliceai_path)
    ht = ht.rows()
    ht = ht.annotate(spliceai = ht.info.SpliceAI[0].split("\\|"))
  
    # extract values 
    ht = ht.annotate(SpliceAI = hl.struct(
        ALLELE = ht.spliceai[0],
        SYMBOL = ht.spliceai[1],
        DS_AG = hl.float32(ht.spliceai[2]),
        DS_AL = hl.float32(ht.spliceai[3]),
        DS_DG = hl.float32(ht.spliceai[4]),
        DS_DL = hl.float32(ht.spliceai[5]),
        DP_AG = hl.int32(ht.spliceai[6]),
        DP_AL = hl.int32(ht.spliceai[7]),
        DP_DG = hl.int32(ht.spliceai[8]),
        DP_DL = hl.int32(ht.spliceai[9])
    ))
    ht = ht.drop(ht.info, ht.spliceai)    
    
    # aggregate information so it's easier to
    ht = ht.transmute(
        SpliceAI=ht.SpliceAI.annotate(
            DS_max=hl.max(
                hl.array([
                   ht.SpliceAI.DS_AG,
                   ht.SpliceAI.DS_AL,
                   ht.SpliceAI.DS_DG,
                   ht.SpliceAI.DS_DL
                ])),
            DS_AX_max=hl.max(
                hl.array([
                   ht.SpliceAI.DS_AG,
                   ht.SpliceAI.DS_AL
                ])),
            DS_DX_max=hl.max(
                hl.array([
                   ht.SpliceAI.DS_DG,
                   ht.SpliceAI.DS_DL
                ]))
        )
    )      
    ht.write(out_prefix + ".ht")

if __name__=='__main__':
    parser = argparse.ArgumentParser()
    # initial params
    parser.add_argument('--spliceai_path', default=None, help='Path to input')
    parser.add_argument('--chrom', default=None, help='chromosome')
    parser.add_argument('--out_prefix', default=None, help='Path prefix for output dataset')

    args = parser.parse_args()

    main(args)



