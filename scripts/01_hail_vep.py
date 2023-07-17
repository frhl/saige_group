#!/usr/bin/env python3

import hail as hl
import argparse
import pandas
import os

from gnomad.utils.vep import process_consequences
from ukb_utils import hail_init
from ko_utils import io

def main(args):

    # parser
    input_path = args.input_path
    out_prefix = args.out_prefix

    # setup flags
    hail_init.hail_bmrc_init_local('logs/hail/hail_format.log', 'GRCh38')
    hl._set_flags(no_whole_stage_codegen='1') # from zulip
    if "bim" in input_path:
        ht = hl.import_table(input_path, no_header=True)
        ht = ht.rename({'f0': 'contig', 'f1': 'rsid', 'f2': 'cm_position', 'f3': 'position', 'f4': 'allele1', 'f5': 'allele2'})
        ht = ht.key_by(locus=hl.locus(ht.contig, hl.int32(ht.position), reference_genome='GRCh38'), alleles=[ht.allele1, ht.allele2])
    elif "vcf" in input_path:
        contig_recoding = {str(i): f"chr{i}" for i in list(range(1, 23)) + ['X']}
        mt = hl.import_vcf(input_path, reference_genome='GRCh38', contig_recoding=contig_recoding)
        ht = mt.rows().select()
    else:
        ht = hl.read_table(input_path)
    ht = hl.vep(ht, "utils/configs/vep105.json")

    def convert_revel_scores_to_unique_float(table):
        # Define a transformation function
        def transform_string_to_float_array(s):
            parts = hl.str(s).split(',')

            float_parts = parts.map(
                lambda x: hl.if_else((x != ".") & hl.is_defined(hl.float64(x)), hl.float64(x), hl.null('float64'))
            ).filter(lambda x: hl.is_defined(x))

            return float_parts.first()

        # Apply the transformation function to the revel_score field
        new_table = table.annotate(
            vep = table.vep.annotate(
                transcript_consequences = table.vep.transcript_consequences.map(
                    lambda tc: tc.annotate(
                        revel_score = transform_string_to_float_array(tc.revel_score)
                    )
                )
            )
        )

        return new_table

    ht = convert_revel_scores_to_unique_float(ht)
    ht = process_consequences(ht)
    # write out VEP hail table
    ht.write(out_prefix + ".vep.ht", overwrite=True)


if __name__=='__main__':
    parser = argparse.ArgumentParser()
    # initial params
    parser.add_argument('--input_path', default=None, help='Path to input')
    parser.add_argument('--out_prefix', default=None, help='Path prefix for output dataset')

    args = parser.parse_args()

    main(args)





