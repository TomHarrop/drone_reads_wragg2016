#!/usr/bin/env python3

import pandas

###########
# GLOBALS #
###########

run_info_file = 'data/SraRunInfo.csv'

sra_container = 'shub://TomHarrop/singularity-containers:sra_2.9.2'
bbduk_container = 'shub://TomHarrop/singularity-containers:bbmap_38.00'

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
        expand('output/fastq_repaired/{sample_name}_{r}.fastq.gz',
               sample_name=all_samples,
               r=[1, 2])

rule repair:
    input:
        r1 = 'output/fastq/{sample_name}/{sample_name}_1.fastq',
        r2 = 'output/fastq/{sample_name}/{sample_name}_2.fastq'
    output:
        r1 = 'output/fastq_repaired/{sample_name}_1.fastq.gz',
        r2 = 'output/fastq_repaired/{sample_name}_2.fastq.gz'
    threads:
        1
    resources:
        mem_gb = 50
    log:
        'output/logs/repair/{sample_name}.log'
    singularity:
        bbduk_container
    shell:
        'repair.sh '
        'in={input.r1} '
        'in2={input.r2} '
        'out={output.r1} '
        'out2={output.r2} '
        'zl=9 '
        'repair=t '
        '-Xmx{resources.mem_gb}g '
        '2> {log}'


rule dump_fastq:
    input:
        'output/SRAs/{sample_name}.sra'
    output:
        r1 = temp('output/fastq/{sample_name}/{sample_name}_1.fastq'),
        r2 = temp('output/fastq/{sample_name}/{sample_name}_2.fastq'),
        tmpdir = temp(directory('output/fastq/tmp_{sample_name}'))
    priority:
        1
    threads:
        48
    params:
        outdir = 'output/fastq/{sample_name}'
    log:
        'output/logs/dump_fastq/{sample_name}.log'
    singularity:
        sra_container
    shell:
        'fasterq-dump '
        '--outfile {wildcards.sample_name} '
        '--outdir {params.outdir} '
        '--temp {output.tmpdir} '
        '--threads {threads} '
        '--details '
        '--split-files '
        '--log-level 5 '
        '{input} '
        '&> {log} '

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
