import pandas as pd 
import numpy as np
import matplotlib.pyplot as plt 
from scipy import stats
import math

plt.style.use('fivethirtyeight')


def cluster_variables(all_variables, subset):
    return np.array([all_variables[i].tolist() for i in range(subset[0], subset[1]+1)])

def clean_data(frame, column, term=-666666666.0):
    frame = frame[~(frame[column] == term)]
    return frame


def clean_columns(frame, cluster):
    frame = frame.rename(columns={cluster[i-8][1]: cluster[i-8][1].split('!!')[3].replace(' ', '_') for i in range(8, 8+len(cluster))})
    return frame

def plot_hist(ax, column, name):
    ## no Nan values
    new_column = column[~np.isnan(column)]
    ax.hist(new_column[1:], bins=100)
    ax.axvline(new_column[0], color='red')
    ax.set_title(name, fontsize = 12)
    ax.set_ylabel('Frequency')

def plot_cluster(cluster, fname, n_row, n_cols):
    fig = plt.figure(figsize=(20, 9))
    for i in range(9, len(cluster.columns)):
        plot_hist(fig.add_subplot(n_row, n_cols, i-8), cluster.iloc[:, i], cluster.columns[i])
    plt.savefig(fname)
    plt.show()

if __name__ == '__main__':
    health_data = pd.read_csv('data/mesa_health_data.csv')
    industry_data = pd.read_csv('data/mesa_industry_data.csv')
    commute_data = pd.read_csv('data/mesa_commute_data.csv')
    income_data = pd.read_csv('data/mesa_income_benefits_data.csv')
    econ_df = pd.read_csv('data/econ_var_names.csv')
    econ_var_names = econ_df.to_numpy()
    

    econ_clusters = {'Industry': (31, 43), 'Commute': (17, 22), 'Income_Benefits': (50, 59), 'Health_Insurance': (94, 96)}
    industry = cluster_variables(econ_var_names, econ_clusters['Industry'])
    commute = cluster_variables(econ_var_names, econ_clusters['Commute'])
    income_benefits = cluster_variables(econ_var_names, econ_clusters['Income_Benefits'])
    health = cluster_variables(econ_var_names, econ_clusters['Health_Insurance'])

    industry_data = clean_data(industry_data, 'Percent Estimate!!INDUSTRY!!Civilian employed population 16 years and over!!Manufacturing')
    commute_data = clean_data(commute_data, 'Percent Estimate!!COMMUTING TO WORK!!Workers 16 years and over!!Walked')
    income_data = clean_data(income_data, 'Percent Estimate!!INCOME AND BENEFITS (IN 2017 INFLATION-ADJUSTED DOLLARS)!!Total households!!Less than $10 000')


    industry_data = clean_columns(industry_data, industry)
    commute_data = clean_columns(commute_data, commute)
    income_data = clean_columns(income_data, income_benefits)
    # income_data = income_data.rename(columns={income_data.columns[i].replace('$', '') for i in range(9, 9+len(income_benefits))})

    plot_cluster(income_data, 'viz/income_viz.png', 2, 5)

    # fig = plt.figure(figsize=(20, 12))
    # for i in range(9, len(income_data.columns)):
    #     plot_hist(fig.add_subplot(5, 2, i-8), income_data.iloc[:, i], income_data.columns[i])
    # plt.savefig('viz/income_viz.png')
    # plt.show()


    # fig = plt.figure(figsize=(20, 12))
    # ax = fig.add_subplot(1, 1,1)
    # ax.hist(income_data.iloc[:, 9][1:], bins = 100)
    # plt.show()


    


