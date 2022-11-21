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

sample = pd.DataFrame(columns=["numThread", "innodb", "lineairdb", "fence", "myisam"])
for db in ["innodb", "lineairdb", "fence", "myisam"]:
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

sample["numThread"] = pd.DataFrame(data=list(range(1,numcores + step, step)))
first_column_data = sample[sample.keys()[0]]
second_column_data = sample[sample.keys()[1]]
third_column_data = sample[sample.keys()[2]]
forth_column_data = sample[sample.keys()[3]]
fifth_column_data = sample[sample.keys()[4]]

fig, ax = plt.subplots()

innoplot = ax.plot(first_column_data, second_column_data, marker='o')
lineairplot = ax.plot(first_column_data, third_column_data, marker='o', linestyle="dashed")
# fenceplot = ax.plot(first_column_data, forth_column_data, marker='o', linestyle="dashed")
isamplot = ax.plot(first_column_data, fifth_column_data, marker='o', linestyle="dashed")
ax.set_xlabel("Number of Threads")
ax.set_ylabel("Throughput (req/sec)")
# ax.legend((innoplot[0], lineairplot[0], fenceplot[0], isamplot[0]), ("InnoDB", "LineairDB", "LineairDB+fence", "MyIsam"), loc=4)
ax.legend((innoplot[0], lineairplot[0], isamplot[0]), ("InnoDB", "LineairDB", "MyIsam"), loc=3)
ax.set_ylim(bottom=0)
fig.savefig("plot.png")