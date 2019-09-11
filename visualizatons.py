import pandas as pd 
import numpy as np
import matplotlib.pyplot as plt 
from scipy import stats
import math
import requests
import ast

plt.style.use('fivethirtyeight')


def cluster_variables(all_variables, subset):
    return np.array([all_variables[i].tolist() for i in range(subset[0], subset[1]+1)])

def clean_data(frame, term=-666666666.0):
    for i in frame.columns:
        frame = frame[~(frame[i] == term)]
    return frame


def clean_columns(frame, cluster):
    frame = frame.rename(columns={cluster[i-8][1]: cluster[i-8][1].split('!!')[3].replace(' ', '_') for i in range(8, 8+len(cluster))})
    return frame


class Grapher(object):
    '''
    Args:
        data: Pandas dataframe to be graphed

        cluster: array of variable names that are going to be graphed. Used to make call to national api to get the national mean of each census variable within a census cluster

        fname: (str) name of file of the graph that will be saved

        fig_rows: (int) number of rows in the subplot for each census variable

        fig_cols: (int) number of columns in the subplot for each census variable

        size_x: (int) x component of the figure size

        size_y: (int) y component of the figure size
    '''
    
    def __init__(self, data, cluster, fname, fig_rows, fig_cols, size_x, size_y):
        self.data = data
        self.cluster = cluster
        self.fname = fname
        self.fig_rows = fig_rows
        self.fig_cols = fig_cols
        self.size_x = size_x
        self.size_y = size_y 

    def _national_vars(self):
        '''
        Args:
            self.cluster : array of census variable names that are going to be graphed

        Returns: 
            results : array of national means for each census variable
        
        '''
        results = []
        for i in range(len(self.cluster)):
            search_term = self.cluster[i][0]
            results.append(self._national_call_api(search_term))
        return results

    
    def _national_call_api(self, search_term, key="2f321eb597c3d3e59dfa9aa2f694622639dee6fc"):
        '''
        Args:
            search_term : (str) name of census variable to be queried 

        Returns: 
            isolated_value : (float) value gathered from query 
        
        '''
        query = "https://api.census.gov/data/2017/acs/acs5/profile?get=NAME,{}&for=us:1".format(search_term)
        call = requests.get(query).text
        clean_call = ast.literal_eval(call)
        isolated_value =  float(clean_call[1][1])
        return isolated_value

        
    def plot_cluster(self):
        '''
        We use basically all of the class attributes here to call _national_mean, plot each census variable, and save the graph. 
        Nothing is returned.
        '''
        national_mean = self._national_vars()
        fig = plt.figure(figsize=(self.size_x, self.size_y))
        for i in range(9, len(self.data.columns)):
            self._plot_hist(fig.add_subplot(self.fig_rows, self.fig_cols, i-8), self.data.iloc[:, i], self.data.columns[i], national_mean[i-9])
        plt.savefig(self.fname)
        plt.show()


    def _plot_hist(self, ax, column, name, national):
        ## no Nan values
        new_column = column[~np.isnan(column)]
        ax.hist(new_column, bins=100)
        ax.axvline(national, color='red')
        ax.set_title(name, fontsize = 12)
        # ax.set_ylabel('Frequency')




if __name__ == '__main__':
    ## Load Data
    health_data = pd.read_csv('data/ohaver_health_data.csv')
    industry_data = pd.read_csv('data/mesa_industry_data.csv')
    commute_data = pd.read_csv('data/mesa_commute_data.csv')
    income_data = pd.read_csv('data/mesa_income_benefits_data.csv')
    vet_data = pd.read_csv('data/mesa_vet_data.csv')
    internet_data = pd.read_csv('data/mesa_internet_data.csv')
    age_data = pd.read_csv('data/ohaver_age_data.csv')

    econ_df = pd.read_csv('data/econ_var_names.csv')
    social_df = pd.read_csv('data/social_var_names.csv')
    demo_df = pd.read_csv('data/demo_var_names.csv')
    econ_var_names = econ_df.to_numpy()
    social_var_names = social_df.to_numpy()
    demo_var_names = demo_df.to_numpy()
    
    ## Census Variable Clusters
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

    demo_clusters = {'Age': (3, 15), 'Gender': (0, 1), 'Race': (35, 54), 'Latino': (71, 74)}
    age = cluster_variables(demo_var_names, demo_clusters['Age'])
    gender = cluster_variables(demo_var_names, demo_clusters['Gender'])
    race = cluster_variables(demo_var_names, demo_clusters['Race'])

    ## Clean Data
    health_data = clean_data(health_data)
    industry_data = clean_data(industry_data)
    commute_data = clean_data(commute_data)
    income_data = clean_data(income_data)
    vet_data = clean_data(vet_data)
    internet_data = clean_data(internet_data)
    age_data = clean_data(age_data)

    health_data = clean_columns(health_data, health)
    industry_data = clean_columns(industry_data, industry)
    commute_data = clean_columns(commute_data, commute)
    income_data = clean_columns(income_data, income_benefits)
    vet_data = clean_columns(vet_data, vet_status)
    internet_data = clean_columns(internet_data, internet)
    age_data = clean_columns(age_data, age)

    ## Graph
    graph_obj = Grapher(age_data, age, 'ohaver_viz/age_viz.png', 13, 1, 20, 10)
    graph = graph_obj.plot_cluster()



    # income_data = income_data.rename(columns={income_data.columns[i].replace('$', '') for i in range(9, 9+len(income_benefits))})

    # plot_cluster(internet, internet_data, 'viz/internet_viz.png', 1, 2, 20, 7)

    # fig = plt.figure(figsize=(20, 12))
    # for i in range(9, len(income_data.columns)):
    #     plot_hist(fig.add_subplot(5, 2, i-8), income_data.iloc[:, i], income_data.columns[i])
    # plt.savefig('viz/income_viz.png')
    # plt.show()

    # x = internet_data.iloc[:, 10][~np.isnan(internet_data.iloc[:, 10])]
    # fig = plt.figure(figsize=(20, 12))
    # ax = fig.add_subplot(1, 1,1)
    # ax.hist(x[:990], bins = 100)
    # plt.show()


    


