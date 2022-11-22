import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.axes as axs
import glob
from IPython import embed
import multiprocessing
import os
import argparse



def makeDFfromCSV(clm, xaxis) :
    sample = pd.DataFrame(columns=clm)
    for db in args.engine:
        csvlist = glob.glob(f'/home/tatsu/LineairDB-storage-engine/bench/results/{db}/*/*results.csv')
        csvlist = sorted(csvlist,key=lambda x: os.path.basename(x))
        clm = []
        for f in csvlist:
            df = pd.read_csv(f)

            # drop rows that are measuring warmup phase
            df = df.drop(df.index[[0,1]])
            # drop rows that may include data from incomplete run
            df = df.drop(df.index[[-1]])

            mean = df["Throughput (requests/second)"].mean()
            clm.append(mean)
        sample[db] = pd.DataFrame(data=clm)

    sample["numThread"] = pd.DataFrame(data=xaxis)
    return sample

def genplot(sample, engine) :
    fig, ax = plt.subplots()
    plot = []
    for c in engine :
        plot.append(ax.plot(sample["numThread"], sample[c], marker='o', label=c))

    ax.set_xlabel("Number of Threads")
    ax.set_ylabel("Throughput (req/sec)")
    ax.legend()
    ax.set_ylim(bottom=0)
    fig.savefig("plot.png")

def main(args) :
    clm = ["numThread"] + args.engine
    sample = makeDFfromCSV(clm, args.xaxis)
    genplot(sample, args.engine)
    
    


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Plot thread vs throughput')
    parser.add_argument('--engine', metavar='DB', type=str, nargs='*',
                        help='storage engine to plot', default=["innodb", "lineairdb"])
    parser.add_argument('--xaxis', metavar='N', type=int, nargs='*',
                        help='step of the xaxis', 
                        default=list(range(1, multiprocessing.cpu_count() + 1, 1)))
    args = parser.parse_args()
    main(args)