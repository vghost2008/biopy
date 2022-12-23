import os
import sys
import glob
import argparse

def parse_args():
    parser = argparse.ArgumentParser(description='Find')
    parser.add_argument('data_dir', type=str,help='data dir')
    parser.add_argument('info', type=str, help='search info')
    parser.add_argument('--suffix', type=str,default=".fasta", help='suffix')
    parser.add_argument('--nr', type=int,default=3000, help='data length')
    parser.add_argument('--info_len', type=int,default=0, help='info length')
    return parser.parse_args()
'''
A<->T
G<->C
'''
def read_file(path):
    with open(path,"r") as f:
        _lines = f.readlines()
        lines = [line.upper() for line in _lines]
    return lines

def reverse_info(info):
    trans_dict = {"A":"T","T":"A","G":"C","C":"G"}
    res = ""
    for x in info[::-1]:
        res += trans_dict[x]
    return res

def find_data(lines,infos,nr=3000):

    for i,line in enumerate(lines):
        for info in infos:
            if info in line:
                idx = line.index(info)
                beg_pos = max(idx-nr,0)
                end_pos = min(idx+nr,len(line))
                data = line[beg_pos:end_pos]
                print(f"Find {info} in line {i}, idx={idx}, idx in data {data.index(info)}")
                print(data)

def find_in_dir(dir_path,info,nr=2000,suffix=".fasta"):
    info = info.upper()
    infos = [info,reverse_info(info)]
    print(f"Infos {infos}")
    for file in glob.glob(os.path.join(dir_path,"*"+suffix)):
        try:
            print(f"Process {file}")
            datas = read_file(file)
            find_data(datas,infos,nr)
        except:
            print(f"Process faild.")

if __name__ == "__main__":
    args = parse_args()
    dir_path = args.data_dir
    info = args.info
    suffix = args.suffix
    nr = args.nr

    if args.info_len>0:
        info = info[:args.info_len]
        print(f"Clip info from {args.info} to {info}")
    elif args.info_len<0:
        info = info[args.info_len:]
        print(f"Clip info from {args.info} to {info}")

    find_in_dir(dir_path,info,suffix=suffix,nr=nr)

