#!/usr/bin/env python3

import hail as hl
import argparse

from gnomad.utils.vep import process_consequences
from ukb_utils import hail_init
from ko_utils import io

def main(args):

    # parser
    input_path = args.input_path
    input_type = args.input_type
    out_prefix = args.out_prefix

    # setup flags
    hail_init.hail_bmrc_init_local('logs/hail/hail_format.log', 'GRCh38')
    hl._set_flags(no_whole_stage_codegen='1') # from zulip
    if input_type in "ht":
        ht = hl.read_table(input_path)
    elif input_type in "bim":
        bim = hl.import_table(input_path, delimiter='\t', no_header=True)
        ht = bim.key_by(
            locus=hl.locus(bim['f0'], hl.int(bim['f3']), reference_genome='GRCh38'),
            alleles=hl.array([bim['f5'], bim['f4']])
        ).select()
    ht = process_consequences(hl.vep(ht, "utils/configs/vep105.json"))
    # write out VEP hail table
    ht.write(out_prefix + ".vep.ht", overwrite=True)


if __name__=='__main__':
    parser = argparse.ArgumentParser()
    # initial params
    parser.add_argument('--input_path', default=None, help='Path to input')
    parser.add_argument('--input_type', default=None, help='Input type, either "ht" or "bim"')
    parser.add_argument('--out_prefix', default=None, help='Path prefix for output dataset')

    args = parser.parse_args()

    main(args)



