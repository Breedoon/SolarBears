from colour import Color
import matplotlib.pyplot as plt
import numpy as np
from dateutil.relativedelta import relativedelta
import os

PLOTS_DIR = 'dataviz'

if not os.path.exists(PLOTS_DIR):
    os.makedirs(PLOTS_DIR)

colors = []


# polynomial approximation
def best_fit_curve(x, y, deg, n):
    coeffs = np.polyfit(x, y, deg)
    x2 = np.linspace(min(x), max(x), len(x) * n)
    y2 = np.polyval(coeffs, x2)
    return x2, y2, coeffs


#  smoothens the data by making each point the mean of all points within 'r' points around it
def normalize(y, r=30):
    new_y = list()
    for i in range(len(y)):
        if i < r:
            new_y.append(np.mean(y[0:i + r]))
        elif i >= len(y) - r:
            new_y.append(np.mean(y[i - r:len(y)]))
        else:
            new_y.append(np.mean(y[i - r:i + r]))
    return new_y


def get_color(i, n):
    global colors
    if len(colors) == 0:
        colors = [c.get_hex_l() for c in Color('red').range_to(Color('purple'), n)]
    return colors[i]


# normalized root mean sqiare error
def nrmse(production, prediction):
    return np.sqrt(np.mean((production - prediction) ** 2)) / (np.mean(production))


# normalized maximum absolute error
def nmae(production, prediction):
    return max(abs(production - prediction) / (max(production) - min(production)))


# removes times with no production in the beginning and end of a production day
#       __                 __
#      /  \         ->    /  \
# ____|____|_____       _|____|_
def cut_production(production, x):  # TODO: make binary search
    start_i = -1
    end_i = -1
    for i in range(len(production)):
        if start_i == -1 and production[i] != 0:
            start_i = i - 1
        if start_i != -1 and production[i] == 0:
            end_i = i + 1
            break
    return production[start_i:end_i], x[start_i:end_i]


# plots predicted and actual production
def plot_error(x, y, y_pred, error, coeff, name):
    plt.plot(x, y, c='k')
    plt.plot(x, y_pred, c='r', linestyle='--')
    plt.title(name + "\nError: " + str(round(error, 5)) + "\nCoeffs: " + str([round(c, 5) for c in coeff]))
    plt.show()


def get_errors(production, entries_per_day=24, error_function=nrmse):  # TODO: find entries_per_day based on dates
    x = np.linspace(0, 24, entries_per_day, endpoint=False)
    prod_pred = [] * (len(production) // entries_per_day)

    for i in range(len(production) // entries_per_day):
        prod, x1 = cut_production(production[entries_per_day * i: entries_per_day * (i + 1)], x)
        x2, pred, coeffs = best_fit_curve(x1, prod, 4, 1)
        prod_pred.append(
            (x1, prod, pred, error_function(prod, pred), coeffs, production.index[entries_per_day * i].split(' ')[0]))

    return sorted(prod_pred, key=lambda l: l[3])


#  len(production) % entries_per_day == 0 TODO: legend
def plot_days(production, entries_per_day=24, filename='day_plot'):
    fig, ax = plt.subplots()
    x = np.linspace(0, 24, entries_per_day, endpoint=False)
    for i in range(len(production) // entries_per_day):
        ax.plot(x, production[entries_per_day * i: entries_per_day * (i + 1)], alpha=0.5, linewidth=1,
                color=get_color(entries_per_day * i, len(production)))

    plt.title(filename)
    fig.show()
    fig.savefig(PLOTS_DIR + "/" + filename + ".png", dpi=1000)


# 'start' and 'end' - datetime or timestamp
def time_batches(start, end, interval={'hours': 1}, include_end=False):
    while end > start:
        yield start, start + relativedelta(**interval)
        start += relativedelta(**interval)
    if include_end:  # when start == end, yield end
        start, start + relativedelta(**interval)
