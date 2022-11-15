import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.axes as axs
import glob
from IPython import embed
import multiprocessing
import os


# TODO: adjust code for "step" once this script would be run on many-core machines
numcores = multiprocessing.cpu_count()
step = 1

sample = pd.DataFrame(columns=["numThread", "innodb", "lineairdb"])
for db in ["innodb", "lineairdb"]:
    csvlist = glob.glob(f'bench/results/{db}/*/*results.csv')
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

sample["numThread"] = pd.DataFrame(data=list(range(1,numcores + step, step)))
first_column_data = sample[sample.keys()[0]]
second_column_data = sample[sample.keys()[1]]
third_column_data = sample[sample.keys()[2]]

fig, ax = plt.subplots()

innoplot = ax.plot(first_column_data, second_column_data, marker='o')
lineairplot = ax.plot(first_column_data, third_column_data, marker='o', linestyle="dashed")
ax.set_xlabel("Number of Threads")
ax.set_ylabel("Throughput (req/sec)")
ax.legend((innoplot[0], lineairplot[0]), ("InnoDB", "LineairDB"), loc=2)
ax.set_ylim(bottom=0)
fig.savefig("test.png")