import os
import sys
import glob
import argparse


def parse_args():
    parser = argparse.ArgumentParser(description='Find')
    parser.add_argument('file_path', type=str,help='data dir')
    parser.add_argument('--split', type=str, default=",",help='search info')
    parser.add_argument('--skip_nr', type=int, default=0,help='skip line no.')

    return parser.parse_args()

def trans_file(file_path,split,skip_nr=0):
    dir_name = os.path.dirname(file_path)
    base_name = os.path.basename(file_path)
    save_path = os.path.join(dir_name,"new_"+base_name)
    with open(file_path,"r") as f:
        lines = f.readlines()
        lines = [x.strip() for x in lines]
    with open(save_path,"w") as f:
        if skip_nr>0:
            for x in lines[:skip_nr]:
                f.write(x+"\n")
            lines = lines[skip_nr:]
        for i,x in enumerate(lines):
            f.write(x)
            if i%2==0:
                f.write(split)
            else:
                f.write("\n")

if __name__ == "__main__":
    args = parse_args()
    trans_file(args.file_path,split=args.split,skip_nr=args.skip_nr)



