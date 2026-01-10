import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.axes as axs
import glob
from IPython import embed
import multiprocessing
import os
import argparse


def Average(lst):
    if len(lst) == 0:
        return None
    return sum(lst) / len(lst)

def makeDFfromCSV(clm, xaxis):
    sample = pd.DataFrame(columns=clm)
    for db in args.engine:
        clm = []
        for x in xaxis:
            csvlist = glob.glob(
                f'{os.path.dirname(__file__)}/../results/{db}/thread_{x}/*results.csv'
            )
            # csvlist = sorted(csvlist, key=lambda x: os.path.basename(x))
            avg = []
            for f in csvlist:
                df = pd.read_csv(f)

                # drop rows that are measuring warmup phase
                df = df.drop(df.index[[0, 1]])
                # drop rows that may include data from incomplete run
                df = df.drop(df.index[[-1]])

                mean = df["Throughput (requests/second)"].mean()
                avg.append(mean)
            clm.append(Average(avg))
        sample[db] = clm

    sample["numThread"] = xaxis
    return sample


def genplot(sample, engine, fname):
    fig, ax = plt.subplots()
    plot = []
    for c in engine:
        plot.append(ax.plot(sample["numThread"],
                    sample[c], marker='o', label=c))

    ax.set_xlabel("Number of Threads")
    ax.set_ylabel("Throughput (req/sec)")
    ax.legend()
    ax.set_xlim(1, sample.numThread.max())
    ax.set_ylim(bottom=0)
    fig.savefig(f'{fname}.pdf')
    fig.savefig(f'{fname}.png')


def main(args):
    clm = ["numThread"] + args.engine
    sample = makeDFfromCSV(clm, args.xaxis)
    genplot(sample, args.engine, args.fname)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Plot thread vs throughput')
    parser.add_argument('--engine', metavar='DB', type=str, nargs='*',
                        help='storage engine to plot', default=["innodb", "lineairdb"])
    parser.add_argument('--xaxis', metavar='N', type=int, nargs='*',
                        help='step of the xaxis',
                        default=list(range(1, multiprocessing.cpu_count() + 1, 1)))
    parser.add_argument('--fname', metavar='fname', type=str,
                        help='name of output file',
                        default="plot")
    args = parser.parse_args()
    main(args)
