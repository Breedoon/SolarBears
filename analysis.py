import time

import pandas as pd
from site_fetching.get_coordinate import get_coordinates
import geopy.distance
from statsmodels.graphics.tsaplots import plot_acf
import numpy as np
from colour import Color
from scipy.stats import ttest_ind, wilcoxon, ttest_1samp
from scipy import stats
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import plotly.graph_objects as go
import matplotlib.pyplot as plt
import os

from fetchers.fetcher_csv import get_site_production, collect_data
from db.helper_db import run_query, run_queries
from misc.mapper import plot_map
from misc.helper import time_batches, normalize, best_fit_curve
import misc.helper

CACHE_DIR = 'efficiency_cache'
DATAVIZ_DIR = 'dataviz'


def se(a, b):
    return (a.std() ** 2 / a.count() + b.std() ** 2 / b.count()) ** 0.5


# chooses a subset of solectria sites based on farthest solaredge sites
def choose_subset(radius):
    solaredge_sites = pd.read_csv('csv/solaredge_sites.csv')
    solaredge_sites = solaredge_sites[solaredge_sites['state'] == 'MA']  # removing CA sites and Boott Mills

    solectria_sites = pd.read_csv('csv/solectria_sites.csv')
    solectria_sites = solectria_sites[solectria_sites['size'].notna()]
    solectria_sites = solectria_sites[solectria_sites['fetch_id'].notna()]

    solectria_sites = solectria_sites[np.logical_and(np.logical_and(
        solectria_sites['latitude'] > min(solaredge_sites['latitude']) - radius,
        solectria_sites['latitude'] < max(solaredge_sites['latitude']) + radius), np.logical_and(
        solectria_sites['longitude'] > min(solaredge_sites['longitude']) - radius,
        solectria_sites['longitude'] < max(solaredge_sites['longitude']) + radius)
    )]

    return list(map(int, solectria_sites['site_id'].values))


def map_sites(solaredge_sites=None, solectria_sites=None):
    if solaredge_sites is None:
        solaredge_sites = pd.read_csv('csv/solaredge_sites.csv')
    if solectria_sites is None:
        solectria_sites = pd.read_csv('csv/solectria_sites.csv')

    solaredge_sites = solaredge_sites[solaredge_sites['site_id'].isin(solaredge_effs.index)]
    solectria_sites = solectria_sites[solectria_sites['site_id'].isin(solectria_effs.index)]

    # jittering
    solaredge_sites['latitude'] = solaredge_sites['latitude'] + np.random.rand(len(solaredge_sites)) * 0.01
    solaredge_sites['longitude'] = solaredge_sites['longitude'] + np.random.rand(len(solaredge_sites)) * 0.01
    solectria_sites['latitude'] = solectria_sites['latitude'] + np.random.rand(len(solectria_sites)) * 0.01
    solectria_sites['longitude'] = solectria_sites['longitude'] + np.random.rand(len(solectria_sites)) * 0.01
    solaredge_sites.insert(0, 'source', 'solaredge')
    solectria_sites.insert(0, 'source', 'solectria')
    df = pd.concat([solaredge_sites, solectria_sites], ignore_index=True)
    plot_map(df)


def plot_hists(se_avg_eff, sol_avg_eff):
    fig, (ax2, ax1) = plt.subplots(2)
    ax1.hist(sol_avg_eff, bins=20, color='green', density=True, label='Histogram (20 bins)')
    x = np.linspace(min(sol_avg_eff), max(sol_avg_eff), len(sol_avg_eff))
    yy = stats.gaussian_kde(sol_avg_eff.dropna())(x)
    ax1.plot(x, yy, color='yellowgreen', lw=3, label='Kernel Density Estimation')
    ax1.set_xlabel('Annual efficiency')
    ax1.set_ylabel('Density of sites')
    ax1.set_xlim([0, 1])
    ax1.axvline(0.05, color='forestgreen', ls='--', label='Cutoffs')
    ax1.axvline(0.3, color='forestgreen', ls='--')
    ax1.legend()
    ax1.set_title('Solectria')
    ax2.hist(se_avg_eff, bins=15, color='orange', density=True, label='Histogram (10 bins)')
    x = np.linspace(min(se_avg_eff), max(se_avg_eff), len(se_avg_eff))
    yy = stats.gaussian_kde(se_avg_eff.dropna())(x)
    ax2.plot(x, yy, color='coral', lw=3, label='Kernel Density Estimation')
    ax2.set_ylabel('Density of sites')
    ax2.axvline(0.05, color='chocolate', ls='--', label='Cutoffs')
    ax2.axvline(0.3, color='chocolate', ls='--')
    ax2.set_xlim([0, 1])
    ax2.legend()
    ax2.set_title('Solaredge')
    fig.tight_layout(pad=1.0)
    fig.savefig(DATAVIZ_DIR + '/' 'hists.png', dpi=400)
    plt.show()


def plot_annual_performance(solaredge_effs, solectria_effs, std=False, r=15):
    se_x = pd.to_datetime(solaredge_effs.columns)
    se_mean = normalize(solaredge_effs.mean(), r)
    se_std = normalize(solaredge_effs.std(), r)
    plt.plot_date(se_x, se_mean,
                  linestyle='solid', marker=None, label='Solaredge', color='orange')

    if std:
        plt.fill_between(se_x, se_mean - se_std, se_mean + se_std, alpha=0.1, color='orange')

    sol_x = pd.to_datetime(solectria_effs.columns)
    sol_mean = normalize(solectria_effs.mean(), r)
    ssol_std = normalize(solectria_effs.std(), r)
    plt.plot_date(sol_x, sol_mean,
                  linestyle='solid', marker=None, label='Solectria', color='green')
    if std:
        plt.fill_between(sol_x, sol_mean - ssol_std, sol_mean + ssol_std, alpha=0.1, color='green')
    plt.xlabel('Date')
    plt.ylabel('Performance')
    plt.title('Solaredge vs Solectria')
    plt.legend()
    plt.savefig(DATAVIZ_DIR + '/' + "solaredge_vs_solectria.png", dpi=400)
    plt.show()


def plot_each_site(effs, filename=None):
    eff_colors = ["rgba({},{},{},{})".format(*[round(val * 255) for val in c.get_rgb()], 0.4) for c in
                  Color('purple').range_to(Color('red'), round(max(effs.transpose().mean() * 100)) + 1)]
    x = pd.to_datetime(effs.columns)
    fig = go.Figure(
        data=[go.Scatter(x=x, y=normalize(effs.loc[ind]), name=ind,
                         line=dict(width=1.5, color=eff_colors[int(round(effs.loc[ind].mean() * 100))])) for ind in
              effs.index])
    fig.show()
    if filename:
        fig.write_html(DATAVIZ_DIR + '/' + filename + ".html")


def plot_annual_plotly(solaredge_effs, solectria_effs, filename='solaredge_vs_solectria'):
    fig = go.Figure(data=[go.Scatter(x=pd.to_datetime(solaredge_effs.columns), y=normalize(solaredge_effs.mean()),
                                     name='Solaredge', line=dict(width=1.5, color='orange')),
                          go.Scatter(x=pd.to_datetime(solectria_effs.columns), y=normalize(solectria_effs.mean()),
                                     name='Solectria', line=dict(width=1.5, color='green'))])
    fig.show()
    if filename:
        fig.write_html(DATAVIZ_DIR + '/' + filename + ".html")


def plot_differences(solaredge_effs, solectria_effs):
    left, width = 0.13, 0.63
    bottom, height = 0.2, 0.70
    spacing = 0.005
    diffs = ((solaredge_effs.mean() - solectria_effs.mean()) / se(solaredge_effs, solectria_effs))
    plt.figure(figsize=(7, 4))
    ax = plt.axes([left, bottom, width, height])
    ax.plot_date(pd.to_datetime(solectria_effs.columns), diffs, c='pink',
                 alpha=0.5, label='Daily differences')
    ax.plot_date(pd.to_datetime(solectria_effs.columns), normalize(diffs),
                 c='magenta', ls='-', marker=None, lw=3, label='Moving mean (n=30)')
    ax.axhline(0, ls='--', c='k', alpha=0.5)
    ax.axhline(np.mean(diffs), ls='--', c='purple', alpha=0.5,
               label='Overall mean')
    ax.set_xlabel('Date')
    ax.set_ylabel('Standard deviations')
    ax.annotate('Solaredge is better', xy=(diffs.index[len(diffs) // 2], max(diffs)), size=25, color='grey', alpha=0.4,
                ha='center', va='top')
    ax.annotate('Solectria is better', xy=(diffs.index[len(diffs) // 2], min(diffs)), size=25, color='grey', alpha=0.4,
                ha='center')
    ax.grid(alpha=0.3)
    ax.legend(loc='lower center', bbox_to_anchor=(0.5, -0.28),
              fancybox=True, ncol=3)
    plt.title('Difference in performance between Solaredge and Solectria')
    ax_histy = plt.axes([left + width + spacing, bottom, 0.2, height])
    ax_histy.tick_params(direction='in', labelleft=False)
    ax_histy.hist(diffs, orientation='horizontal', color='pink', bins=15, density=True)
    ax_histy.axhline(0, ls='--', c='k', alpha=0.5)
    ax_histy.axhline(np.mean(diffs), ls='--', c='purple', alpha=0.5,
                     label='Overall mean', )
    ax_histy.axis('off')
    x = np.linspace(min(diffs), max(diffs), len(diffs))
    yy = stats.gaussian_kde(diffs.dropna())(x)
    ax_histy.plot(yy, x, color='magenta', lw=3)
    plt.savefig(DATAVIZ_DIR + '/' + 'diffs.png', dpi=400)
    plt.show()


def solectria_to_database(solectria_ids, start, end):
    fetching_times = [] * len(solectria_ids)
    for site_id in solectria_ids:
        print('_____________________\nFetching site:', site_id)
        t = time.time()
        try:
            if not collect_data(site_id, start, end, 60):
                print("Fetching failed")
        except Exception as e:
            print('Error while fetching:', str(e))
        t = time.time() - t
        print("Total time:", t)
        fetching_times.append(t)
    print(solectria_ids)
    print(fetching_times)


def copy_from_main_db():
    from fetchers.fetcher_csv import store
    from math import ceil
    site = run_query('SELECT * FROM site', retrieve=True)
    store('site', site, bulk_load=True)
    component_details = run_query('SELECT * FROM component_details', retrieve=True)
    store('component_details', component_details, bulk_load=True)
    n = run_query('SELECT count(*) FROM production')[0]['count']
    l = 500000
    for i in range(ceil(n / l)):
        production = run_query('SELECT * FROM production LIMIT {} OFFSET {}'.format(l, i * l), retrieve=True)
        production.fillna(0, inplace=True)
        production["value"] = production["value"].astype(int)
        store('production', production, bulk_load=True)


# converts production to power
# production - dataframe with columns 'value' and 'date' for production in Wh and date in format '2020-01-20 10:00:00'
# if interval == None, assumes production is sorted by 'date', and calculates interval based on first two 'date's
def df_to_power(production, interval=None):
    # time interval in seconds for conversion to watts; based on production[1] - production[0]
    if not interval:
        interval = sum((np.array(list(map(int, str(production['date'][1]).split(' ')[1].split(':'))))
                        - np.array(list(map(int, str(production['date'][0]).split(' ')[1].split(':')))))
                       * np.array([3600, 60, 1]))
    # efficiency = W / W_max; W = Wh / h; W_max = system size * 1000 (kW to W)
    production['value'] = production['value'].div(interval / 3600)
    # production['value'].fillna(0, inplace=True)
    return production


def df_to_efficiency(production, size, interval=None):
    return df_to_power(production).div(size * 1000)


# production - dataframe with columns 'value' and 'date' for production in Wh and date in format '2020-01-20 10:00:00'
# interval in seconds
def daily_efficiency(production, size, interval):
    efficiency = df_to_efficiency(production, size, interval)
    result = {}
    n_points = {}
    for eff, date in zip(efficiency['efficiency'], efficiency['date']):
        try:
            result[datetime(date.year, date.month, date.day)] += eff
            n_points[datetime(date.year, date.month, date.day)] += 1
        except KeyError:
            result[datetime(date.year, date.month, date.day)] = eff
            n_points[datetime(date.year, date.month, date.day)] = 1
    for day in result:
        result[day] = result[day] / n_points[day]
    return pd.DataFrame(data=list(result.values()), index=result.keys(), columns=['value'])


# returns a file name to store daily efficiencies for a given site for given time period
def get_filename(site_id, start, end):
    return CACHE_DIR + '/' + "_".join([str(site_id), str(start), str(end)]).replace(':', '=') + '.csv'


# conducts a two-tailed t-test comparing two
# 'sites_1' and 'sites_2' - lists of site_ids; 'start', 'end' - datetime objects
# interval in minutes
def average_daily_efficiency(site_ids, start, end, interval):
    site_ids = list(map(str, site_ids))

    site_metadatas = run_query(
        'SELECT site_id, size FROM site '
        'WHERE site_id IN ' + '(' + ("'{}', " * len(site_ids))[:-2].format(*site_ids) + ')', True).set_index('site_id')
    system_sizes = [float(site_metadatas['size'][str(id)]) for id in site_ids]

    if not os.path.exists(CACHE_DIR):
        os.makedirs(CACHE_DIR)
    existing_files = [CACHE_DIR + '/' + f for f in os.listdir(CACHE_DIR)]
    expected_files = [get_filename(site_id, start, end) for site_id in site_ids]

    efficiencies = [None] * len(site_ids)
    new_sites = {}

    for i in range(len(site_ids)):
        if expected_files[i] not in existing_files:
            new_sites[site_ids[i]] = i
        else:
            efficiencies[i] = pd.read_csv(expected_files[i]).set_index('date')
    if len(new_sites) > 0:
        new_datas = run_query(
            "SELECT site_id, date, value FROM pss.public.production "
            "WHERE date >= '{start}'::timestamp and date < '{end}'::timestamp AND "
            .format(start=str(start), end=str(end)) +
            "site_id IN " + "(" + ("'{}', " * len(new_sites))[:-2].format(*new_sites) + ')', True)

        daily_power_per_site = {}
        n_elements_per_site = {}
        daily_efficiency_per_site = {}
        powers = df_to_power(new_datas, interval * 60)
        for site_id, date, value in zip(powers['site_id'], powers['date'], powers['value']):
            if not daily_power_per_site.get(site_id):
                daily_power_per_site[site_id] = {}
                n_elements_per_site[site_id] = {}
            day = date._date_repr
            if not daily_power_per_site[site_id].get(day):
                daily_power_per_site[site_id][day] = value
                n_elements_per_site[site_id][day] = 1
                continue
            daily_power_per_site[site_id][day] += value
            n_elements_per_site[site_id][day] += 1

        for site_id in daily_power_per_site:
            daily_efficiency_per_site[site_id] = {}
            for day in daily_power_per_site[site_id]:
                daily_efficiency_per_site[site_id][day] = daily_power_per_site[site_id][day] \
                                                          / n_elements_per_site[site_id][day] \
                                                          / (system_sizes[new_sites[site_id]] * 1000)
            efficiencies[new_sites[site_id]] = pd.DataFrame(data=list(daily_efficiency_per_site[site_id].values()),
                                                            index=daily_efficiency_per_site[site_id].keys(),
                                                            columns=['value'])
            efficiencies[new_sites[site_id]].to_csv(get_filename(site_id, start, end), index_label='date')

    total_effs = efficiencies[0].rename(columns={"value": site_ids[0]}).transpose()
    for i in range(1, len(efficiencies)):
        try:
            total_effs = total_effs.append(efficiencies[i].rename(columns={"value": site_ids[i]}).transpose(),
                                           sort=True)
        except AttributeError:
            print("No data for site", site_ids[i])
    total_effs.fillna(0, inplace=True)

    return total_effs


if __name__ == '__main__':
    solaredge_ids = sorted(list(zip(*run_query(
        "SELECT site_id FROM site WHERE length(site_id) > 4 AND state = 'MA'", True).values))[0])
    # solectria_ids = sorted(list(set(choose_subset(0.2125)).intersection(set(map(int, list(zip(*run_query(
    solectria_ids = sorted(list(
        {1759, 3970, 1097, 3506, 3929, 4686, 4635, 4221, 4267, 3452, 4031, 4806, 1630, 918, 682, 3750, 715, 3590, 448,
         4034, 3117, 694, 2015, 4065, 3113, 3855, 3988, 4656, 3125, 2550, 4661, 3778, 4721, 3577, 4381, 2026, 4160,
         1700, 1703, 2002, 1701, 3009, 2014, 3347, 3980, 2494, 1702, 4234, 644, 4296, 3859, 3862, 3682, 3680, 2030,
         3793, 2613, 1751, 3116, 1752, 2197, 3858, 2485, 1049, 1982, 2768, 4334, 3585, 3274, 948, 2037, 1603, 3771,
         4731, 3421, 530, 1563, 3654, 3551, 3552, 3227, 4572, 843, 1745, 1740, 1444, 1850, 2177, 3660, 2437, 3558, 3095,
         1881, 1892, 871, 716, 895}.intersection(set(map(int, list(zip(*run_query(
        "SELECT site_id FROM site WHERE length(site_id) <= 4", True).values))[0])))))

    start = datetime(2019, 3, 1)
    end = datetime(2020, 3, 1)

    solectria_effs = average_daily_efficiency(solectria_ids, start, end, 60)
    solaredge_effs = average_daily_efficiency(solaredge_ids, start, end, 15)

    sol_avg_eff = solectria_effs.transpose().mean()
    solectria_effs = solectria_effs[
        solectria_effs.index.isin(sol_avg_eff[np.logical_and(sol_avg_eff > 0.05, sol_avg_eff < 0.3)].index)]
    se_avg_eff = solaredge_effs.transpose().mean()
    solaredge_effs = solaredge_effs[
        solaredge_effs.index.isin(se_avg_eff[np.logical_and(se_avg_eff > 0.05, se_avg_eff < 0.3)].index)]

    solaredge_effs = solaredge_effs.replace(0, np.nan)
    solectria_effs = solectria_effs.replace(0, np.nan)

    diffs = (solaredge_effs.mean() - solectria_effs.mean()) / se(solaredge_effs, solectria_effs)
    diffs.index = pd.to_datetime(diffs.index)
    ttest_1samp(diffs, 0, nan_policy='omit')

    ttest_ind(solaredge_effs.transpose().mean(), solectria_effs.transpose().mean(), nan_policy='omit')

    solectria_size_effs = pd.concat([run_query(
        'SELECT site_id, size FROM site '
        'WHERE site_id IN ' + '(' + ("'{}', " * len(solectria_effs.index))[:-2].format(*solectria_effs.index) + ')',
        True).set_index('site_id'), solectria_effs.transpose().mean()], ignore_index=False, axis=1)

    plot_acf(diffs, lags=len(solectria_effs.columns) - 1, color='magenta', title='', zero=False)
    plt.ylabel('Average correlation coefficient (r)')
    plt.xlabel('Lag time (days)')
    plt.savefig(DATAVIZ_DIR + "/" + 'autocorr.png', dpi=400)
    plt.show()

    plot_differences(solaredge_effs, solectria_effs)
    plot_annual_performance(solaredge_effs, solectria_effs)
    plot_annual_plotly(solaredge_effs, solectria_effs)
    plot_each_site(solaredge_effs)
    plot_each_site(solectria_effs)
