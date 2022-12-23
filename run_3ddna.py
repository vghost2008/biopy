#!/usr/bin/python3
import os.path as osp
import os
import subprocess
import argparse
from subprocess import call
import sys
import time

def parse_args():
    parser = argparse.ArgumentParser(description="build gif")
    parser.add_argument("--fa",type=str,help="fasta file")
    parser.add_argument("--enzyme",type=str,default="HindIII", help="enzyme")
    parser.add_argument("--r1",type=str,help="fastq r1 file")
    parser.add_argument("--r2",type=str,help="fastq r2 file")
    parser.add_argument("--outdir",type=str,help="outdir")
    parser.add_argument("--cpu_num",type=int,default=8,help="cpu number")
    parser.add_argument("--mem",type=int,default=80,help="Memory G")
    args = parser.parse_args()
    return args

def run_process(cmd):
    try:
        out_bytes = subprocess.check_output(cmd,shell=True)
    except subprocess.CalledProcessError as e:
        out_bytes = e.output
    try:
        out_text = out_bytes.decode('utf-8')
    except:
        print(f"decode faild.")
        return ""
    return out_text

def wait_squeue_finish(joblogfile,user="user321"):
    print(f"Job log file {joblogfile}")
    with open(joblogfile,"r") as f:
        while True:
            lines = f.readlines()
            if len(lines)>0:
                for l in lines:
                    sys.stdout.write(l)
                continue
            cmd = f"squeue -u {user}"
            out = run_process(cmd)
            if user not in out:
                return True
            time.sleep(2.0)

def add_env2command(cmd):
    env = "export JAVA_HOME=/data/apps/jdk/jdk1.8.0_131; export CLASSPATH=$JAVA_HOME/lib/; export PATH=\"$JAVA_HOME/bin:$PATH\"; export PATH=\"$HOME/.local/bin:$HOME/bin:/w/00/u/user321/software/bwa-0.7.17:/w/00/u/user321/installed_software/bin:$PATH\"; "
    cmd = env+cmd
    return cmd

def run_command(cmd,add_env=True,add_bash=True):
    if add_bash:
        cmd = " bash "+cmd
    if add_env:
        cmd = add_env2command(cmd)
    print(f"Run {cmd}")
    #subprocess.call(cmd,shell=True)
    os.system(cmd)

def run_python_command(cmd):
    return run_command(cmd,add_env=False,add_bash=False)

def write_queue_job(cmd,cpu_num,mem,outfile,joblogfile,add_env=True):
    cwd = osp.dirname(outfile)

    if add_env:
        cmd = add_env2command(cmd)

    if not osp.exists(cwd):
        os.makedirs(cwd)

    with open(outfile,'w') as f:
        f.write("#!/bin/sh\n")
        f.write("#SBATCH -c "+str(cpu_num)+" --mem "+str(mem)+"G\n")
        f.write("#SBATCH -o "+joblogfile+"\n")
        f.write("#SBATCH -e "+joblogfile+"\n")
        f.write("#SBATCH -D "+cwd+"\n")
        f.write(cmd)
    print(f"Write job file {outfile} finish.")
    return True

def run_queue_job(filepath):
    call(["sbatch", filepath])
    return True

def get_filebase_name(filename):
    return osp.splitext(osp.basename(filename))[0]

def change_filename(filepath,new_name):
    filedir = osp.dirname(osp.abspath(filepath))
    return osp.join(filedir,new_name)

def get_restriction_site_file_path(fa,basename,enzyme):
    rsf = f"{basename}_{enzyme}.txt"
    rsf = change_filename(fa,rsf)
    return rsf

def get_chrom_sizes_file_path(fa,basename):
    csf = f"{basename}.chrom.sizes"
    csf = change_filename(fa,csf)
    return csf

def run_bwa(fa):
    cmd = f"bwa index \"{fa}\""
    cmd = f"/w/00/u/user321/software/bwa-0.7.17/bwa index \"{fa}\""
    run_command(cmd,add_bash=False)
    testfile = fa+".sa"
    if not osp.exists(testfile):
        print(f"Check file {testfile} faild.")
        return False
    else:
        return True

def generate_site_positions(enzyme,basename,fa):
    cmd = f"python ~/software/juicer/misc/generate_site_positions.py {enzyme} {basename} \"{fa}\""
    cwd = osp.dirname(fa)
    cmd = f"cd {cwd}; {cmd}"
    run_python_command(cmd)
    testfile = f"{basename}_{enzyme}.txt"
    testfile = change_filename(fa,testfile)
    if not osp.exists(testfile):
        print(f"Check file {testfile} faild.")
        return False
    else:
        return True


def get_chrom_sizes(enzyme,basename,fa):
    #restriction site file
    infile = get_restriction_site_file_path(fa,basename,enzyme)
    outfile = get_chrom_sizes_file_path(fa,basename)
    cmd = "awk 'BEGIN{OFS=\"\\t\"}{print $1, $NF}' "
    cmd += f"{infile} > {outfile}"
    run_command(cmd,add_env=False,add_bash=False)
    if not osp.exists(outfile):
        print(f"Check file {outfile} faild.")
        return False
    else:
        cmd = f"cat {outfile}"
        print(f"chrom.sizes")
        run_command(cmd,add_env=False,add_bash=False)
        return True

def wait_juicer_start(datadir,joblogfile):
    align_dir = osp.join(datadir,"aligned")
    #splits_dir = osp.join(datadir,"splits")
    while not osp.exists(align_dir):
        time.sleep(1)
    while not osp.exists(joblogfile):
        time.sleep(1)
    print(f"Juice started.")
    return True

def run_juicer(datadir,fa,enzyme,basename,cpu_num=8,mem=80):
    #restriction site file
    rsf = get_restriction_site_file_path(fa,basename,enzyme)
    csf = get_chrom_sizes_file_path(fa,basename)
    '''
    export JAVA_HOME=/data/apps/jdk/jdk1.8.0_131; export CLASSPATH=$JAVA_HOME/lib/; export PATH="$JAVA_HOME/bin:$PATH"; export PATH="$HOME/.local/bin:$HOME/bin:/w/00/u/user321/software/bwa-0.7.17:/w/00/u/user321/installed_software/bin:$PATH"; 
    bash /w/00/u/user321/software/juicer/CPU/juicer.sh -d /w/00/u/user321/data/test -z /w/00/u/user321/data/02_tibet/02.tibet.final.fasta -y /w/00/u/user321/data/02_tibet/02.tibet.final.fasta_HindIII.txt -p /w/00/u/user321/data/02_tibet/02.tibet.final.fasta.chrom.sizes -s HindIII -t 30 -D /w/00/u/user321/software/juicer --assembly
    '''
    cmd = f"~/software/juicer/CPU/juicer.sh -d {datadir} -z {fa} -y {rsf} -p {csf} -s {enzyme} -t 30 -D ~/software/juicer --assembly"
    cmd = osp.expanduser(cmd)
    job_dir = osp.join(datadir,"jobs")
    jobfile = osp.join(job_dir,f"{basename}_{enzyme}.sl")
    joblogfile = jobfile+".out"
    write_queue_job(cmd,cpu_num,mem,jobfile,joblogfile)
    run_queue_job(jobfile)
    wait_juicer_start(datadir,joblogfile)
    wait_squeue_finish(joblogfile,user="user321")
    align_dir = osp.join(datadir,"aligned")
    outfile = osp.join(align_dir,"merged_nodups.txt")
    if osp.exists(outfile):
        return True
    else:
        print(f"Find {outfile} faild.")
        return False

def run_3ddna(datadir,fa):
    outdir = osp.join(datadir,"3ddna")
    os.makedirs(outdir,exist_ok=True)

    align_dir = osp.join(datadir,"aligned")
    nodups = osp.join(align_dir,"merged_nodups.txt")
    cmd = f"~/software/3d-dna-201008/run-asm-pipeline.sh -r 2 {fa} {nodups}"
    cmd = "cd {outdir}; "+cmd

    run_command(cmd)

    return True

def main(args):
    args = parse_args()
    enzyme = args.enzyme
    outdir = osp.abspath(args.outdir)
    if osp.exists(outdir):
        print(f"ERROR: Output dir {outdir} exists")
        return False
    os.makedirs(outdir)
    fasta_dir = osp.join(outdir,"fasta")
    os.makedirs(fasta_dir)
    fa = osp.join(fasta_dir,osp.basename(args.fa))
    os.symlink(args.fa,fa)

    fastq_dir = osp.join(outdir,"fastq")
    os.makedirs(fastq_dir)
    r1_path = osp.join(fastq_dir,"data_R1.fastq.gz")
    r2_path = osp.join(fastq_dir,"data_R2.fastq.gz")
    os.symlink(args.r1,r1_path)
    os.symlink(args.r2,r2_path)

    if not run_bwa(fa):
        return False

    basename = get_filebase_name(fa)
    if not generate_site_positions(enzyme,basename,fa):
        return False

    if not get_chrom_sizes(enzyme,basename,fa):
        return False

    if not run_juicer(outdir,fa,enzyme,basename,cpu_num=args.cpu_num,mem=args.mem):
        return False

    run_3ddna(outdir,fa)


if __name__ == "__main__":
    print(f"PID={os.getpid()}, username={os.getlogin()}")
    args = parse_args()
    main(args)





