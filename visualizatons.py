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

## TODO: draw national data for visualizations
def command_center(national, row, var_names):
    results = []
    if national:
        for i in range(len(var_names)):
            search_term = var_names[i][0]
            results.append([var_names[i][1], (national_call_api(search_term))])
        return results

def national_call_api(search_term, key="2f321eb597c3d3e59dfa9aa2f694622639dee6fc"):
    query = "https://api.census.gov/data/2017/acs/acs5/profile?get=NAME,{}&for=us:1".format(search_term)
    call = requests.get(query).text
    clean_call = ast.literal_eval(call)
    isolated_value =  float(clean_call[1][1])
    return isolated_value

def plot_hist(ax, column, name, national_mean):
    ## no Nan values
    new_column = column[~np.isnan(column)]
    ax.hist(new_column, bins=100)
    # ax.axvline(new_column[0], color='red')
    ax.set_title(name, fontsize = 12)
    ax.set_ylabel('Frequency')

def plot_cluster(cluster, fname, n_row, n_cols, figx, figy):
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
    vet_data = pd.read_csv('data/mesa_vet_data.csv')
    econ_df = pd.read_csv('data/econ_var_names.csv')
    social_df = pd.read_csv('data/social_var_names.csv')
    econ_var_names = econ_df.to_numpy()
    social_var_names = social_df.to_numpy()
    

    econ_clusters = {'Industry': (31, 43), 'Commute': (17, 22), 'Income_Benefits': (50, 59), 'Health_Insurance': (94, 96)}
    industry = cluster_variables(econ_var_names, econ_clusters['Industry'])
    commute = cluster_variables(econ_var_names, econ_clusters['Commute'])
    income_benefits = cluster_variables(econ_var_names, econ_clusters['Income_Benefits'])
    health = cluster_variables(econ_var_names, econ_clusters['Health_Insurance'])

    social_clusters = {'Internet': (149, 150), 'Language': (109, 119), 'Education': (57, 65), 'Veteran_Status': (67, 67)}
    internet = cluster_variables(social_var_names, social_clusters['Internet'])
    language = cluster_variables(social_var_names, social_clusters['Language'])
    education = cluster_variables(social_var_names, social_clusters['Education'])
    vet_status = cluster_variables(social_var_names, social_clusters['Veteran_Status'])

    industry_data = clean_data(industry_data, 'Percent Estimate!!INDUSTRY!!Civilian employed population 16 years and over!!Manufacturing')
    commute_data = clean_data(commute_data, 'Percent Estimate!!COMMUTING TO WORK!!Workers 16 years and over!!Walked')
    income_data = clean_data(income_data, 'Percent Estimate!!INCOME AND BENEFITS (IN 2017 INFLATION-ADJUSTED DOLLARS)!!Total households!!Less than $10 000')
    vet_data = clean_data(vet_data, 'Percent Estimate!!VETERAN STATUS!!Civilian population 18 years and over!!Civilian veterans')

    industry_data = clean_columns(industry_data, industry)
    commute_data = clean_columns(commute_data, commute)
    income_data = clean_columns(income_data, income_benefits)
    vet_data = clean_columns(vet_data, vet_status)
    # income_data = income_data.rename(columns={income_data.columns[i].replace('$', '') for i in range(9, 9+len(income_benefits))})

    plot_cluster(vet_data, 'viz/vet_viz.png', 1, 1, 10, 5)

    # fig = plt.figure(figsize=(20, 12))
    # for i in range(9, len(income_data.columns)):
    #     plot_hist(fig.add_subplot(5, 2, i-8), income_data.iloc[:, i], income_data.columns[i])
    # plt.savefig('viz/income_viz.png')
    # plt.show()


    # fig = plt.figure(figsize=(20, 12))
    # ax = fig.add_subplot(1, 1,1)
    # ax.hist(income_data.iloc[:, 9][1:], bins = 100)
    # plt.show()


    


