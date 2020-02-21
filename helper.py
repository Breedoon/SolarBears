from colour import Color
import matplotlib.pyplot as plt
import numpy as np
from datetime import datetime, timedelta

colors = []

def get_color(i, n):
    global colors
    if len(colors) == 0:
        colors = [c.get_hex_l() for c in Color('red').range_to(Color('purple'), n)]
    return colors[i]


#  len(productions) % entries_per_day == 0
def plot_days(productions, entries_per_day=24, filename='day_plot'):
    fig, ax = plt.subplots()
    x = np.linspace(0, 24, entries_per_day, endpoint=False)
    for i in range(len(productions) // entries_per_day):
        ax.plot(x, productions[entries_per_day * i: entries_per_day * (i + 1)], alpha=0.5, linewidth=1,
                color=get_color(entries_per_day * i, len(productions)))

    plt.title(filename)
    fig.show()
    fig.savefig(filename + ".png", dpi=1000)

# [start, end)
def time_batches(start, end, interval={'hours': 1}):
    while end > start:
        yield start, start + timedelta(*interval)
        start += timedelta(*interval)