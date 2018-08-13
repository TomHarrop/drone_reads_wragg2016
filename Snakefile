#!/usr/bin/env python3

import pandas

###########
# GLOBALS #
###########

run_info_file = 'data/SraRunInfo.csv'

sra_container = 'shub://TomHarrop/singularity-containers:sra_2.9.2'

########
# MAIN #
########

run_info = pandas.read_csv(run_info_file)

# split the run info into name_to_url dict
col_to_sn = run_info.to_dict()['SampleName']
col_to_url = run_info.to_dict()['download_path']

name_to_url = {}
for key in col_to_sn:
    name_to_url[col_to_sn[key]] = col_to_url[key]

# get a list of all names
all_samples = sorted(set(name_to_url.keys()))


#########
# RULES #
#########

rule target:
    input:
        expand('output/fastq/{sample_name}_{r}.fastq.gz',
               sample_name=all_samples,
               r=[1, 2])

rule compress_fastq:
    input:
        'output/fastq/{sample_name}_{r}.fastq',
    output:
        'output/fastq/{sample_name}_{r}.fastq.gz'
    log:
        'output/logs/gzip/{sample_name}_{r}.log'
    priority:
        2
    shell:
        'gzip --best --to-stdout --verbose '
        '{input} > {output} 2> {log}'


rule dump_fastq:
    input:
        'output/SRAs/{sample_name}.sra'
    output:
        r1 = temp('output/fastq/{sample_name}_1.fastq'),
        r2 = temp('output/fastq/{sample_name}_2.fastq')
    priority:
        1
    threads:
        2
    params:
        outdir = 'output/fastq'
    log:
        'output/logs/dump_fastq/{sample_name}.log'
    singularity:
        sra_container
    shell:
        'fasterq-dump '
        '--outfile {wildcards.sample_name} '
        '--outdir {params.outdir} '
        '--threads {threads} '
        '--details '
        '--split-files '
        '--log-level 5 '
        '{input} '
        '&> {log}'

rule download_sra:
    output:
        temp('output/SRAs/{sample_name}.sra')
    params:
        url = lambda wildcards: name_to_url[wildcards.sample_name]
    threads:
        1
    log:
        'output/logs/download_sra/{sample_name}.log'
    shell:
        'wget '
        '-O {output} '
        '{params.url} '
        '&> {log}'
