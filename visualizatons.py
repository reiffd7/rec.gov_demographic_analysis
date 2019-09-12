import pandas as pd 
import numpy as np
import matplotlib.pyplot as plt 
from scipy import stats
import math
import requests
import ast
import random
from scipy import stats

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
        hypo_test: (bool) do we want to do a hypothesis test?
        
        data: Pandas dataframe to be graphed or pandas series if we are graphing a hypothesis test

        cluster: array of variable names that are going to be graphed. Used to make call to national api to get the national mean of each census variable within a census cluster
                one variable name if we are graphing a hypothesis test

        fname: (str) name of file of the graph that will be saved

        fig_rows: (int) number of rows in the subplot for each census variable

        fig_cols: (int) number of columns in the subplot for each census variable

        size_x: (int) x component of the figure size

        size_y: (int) y component of the figure size

    '''
    
    def __init__(self, hypo_test, data, cluster, fname, fig_rows, fig_cols, size_x, size_y):
        self.hypo_test = hypo_test
        self.data = data
        self.cluster = cluster
        self.fname = fname
        self.fig_rows = fig_rows
        self.fig_cols = fig_cols
        self.size_x = size_x
        self.size_y = size_y 
        
    def _national_means(self):
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

    
    def _national_call_api(self, search_term, key="259ba8642bd19b70be7abaee303575bb2435f9e3"):
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

    def _national_distribution(self, search_term, key="259ba8642bd19b70be7abaee303575bb2435f9e3"):
        states = ["%.2d" % i for i in range(1, 57)]
        states.remove('03')
        states.remove('07')
        states.remove('14')
        states.remove('43')
        states.remove('52')
        result = []
        for state in states:
            try:
                query = "https://api.census.gov/data/2017/acs/acs5/profile?get=NAME,{}&for=state:{}".format(search_term, state)
                print('querrying')
                call = requests.get(query).text
                print('cleaning')
                clean_call = ast.literal_eval(call)
                print('isolating')
                isolated_value =  float(clean_call[1][1])
                print(state, isolated_value)
                result.append(isolated_value)
            except:
                import pdb; pdb.set_trace()
        return result

        
    def plot_cluster(self):
        '''
        We use basically all of the class attributes here to call _national_mean, plot each census variable, and save the graph. 
        Nothing is returned.
        '''
        
        fig = plt.figure(figsize=(self.size_x, self.size_y))
        if self.hypo_test:
            search_term = self.cluster[0]
            null_sample = self._national_distribution(search_term)
            self._plot_hypo_test(fig.add_subplot(self.fig_rows, self.fig_cols, 1), null_sample)
            plt.savefig(self.fname)
            plt.show()
        else:
            national_mean = self._national_means()
            for i in range(9, len(self.data.columns)):
                self._plot_hist(fig.add_subplot(self.fig_rows, self.fig_cols, i-8), self.data.iloc[:, i], self.data.columns[i], national_mean[i-9])
            plt.savefig(self.fname)
            plt.show()


    def _plot_hist(self, ax, column, name, national):
        ## no Nan values
        new_column = column[~np.isnan(column)]
        ax.hist(new_column, bins=100)
        ax.axvline(national, color='red')
        # ax.set_title(name, fontsize = 12)
        # ax.set_ylabel('Frequency')

    def _plot_hypo_test(self, ax, null_sample):
        us_dist = np.array(null_sample)
        print(us_dist)
        new_column = self.data[~np.isnan(self.data)]

        samp_mean = np.mean(new_column.to_numpy())
        samp_std = np.std(new_column.to_numpy())/len(new_column)

        us_mean = np.mean(us_dist)
        us_std = np.std(us_dist)/len(us_dist)

        null_dist = stats.norm(loc = us_mean, scale = us_std)
        samp_dist = stats.norm(loc = samp_mean, scale = samp_std)
        lower = null_dist.ppf(0.025)
        upper = null_dist.ppf(0.975)
        diff = 2*np.absolute(us_mean-samp_mean)
        x_values = np.linspace((us_mean - (diff)), (us_mean + (diff)), 250)
        null_pdf = null_dist.pdf(x_values)
        ax.plot(x_values, null_pdf)
        ax.axvline(samp_mean, color='red', linestyle= '--', linewidth=1, alpha = 0.6)
        ax.axvline(lower, color='green', linestyle= '--', linewidth=1, alpha = 0.8)
        ax.axvline(upper, color='green', linestyle= '--', linewidth=1, alpha = 0.8)
        ax.set_title(self.data.name)
        cdf_calc = null_dist.cdf(samp_mean)
        p_value = 1 - cdf_calc
        print(p_value)
        # pass




if __name__ == '__main__':
    ## Load Data
    health_data = pd.read_csv('data/ohaver_health_data.csv')
    industry_data = pd.read_csv('data/mesa_industry_data.csv')
    commute_data = pd.read_csv('data/mesa_commute_data.csv')
    income_data = pd.read_csv('data/mesa_income_benefits_data.csv')
    vet_data = pd.read_csv('data/ohaver_vets_data.csv')
    internet_data = pd.read_csv('data/mesa_internet_data.csv')
    age_data = pd.read_csv('data/ohaver_age_data.csv')
    gender_data = pd.read_csv('data/ohaver_gender_data.csv')

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
    gender_data = clean_data(gender_data)

    health_data = clean_columns(health_data, health)
    industry_data = clean_columns(industry_data, industry)
    commute_data = clean_columns(commute_data, commute)
    income_data = clean_columns(income_data, income_benefits)
    vet_data = clean_columns(vet_data, vet_status)
    internet_data = clean_columns(internet_data, internet)
    age_data = clean_columns(age_data, age)
    gender_data = clean_columns(gender_data, gender)

    ## Graph
    graph_obj = Grapher(True, gender_data.iloc[:, 9], gender[0], 'mesa_viz/hypothesis_test.png', 1, 1, 10, 10)
    graph = graph_obj.plot_cluster()


    # states = ["%.2d" % i for i in range(1, 57)]
    # states.remove('03')
    # states.remove('07')
    # states.remove('14')
    # states.remove('43')
    # states.remove('52')
    result = []
    for state in states:
        try:
            print("trying query")
            query = "https://api.census.gov/data/2017/acs/acs5/profile?get=NAME,{}&for=state:{}".format(gender[0][0], state)
            print("trying call")
            call = requests.get(query).text
            print("trying clean call")
            clean_call = ast.literal_eval(call)
            print("trying isolated_value")
            isolated_value =  float(clean_call[1][1])
            print(state, isolated_value)
            result.append(isolated_value)
        except:
            import pdb; pdb.set_trace()







    # income_data = income_data.rename(columns={income_data.columns[i].replace('$', '') for i in range(9, 9+len(income_benefits))})

    # plot_cluster(internet, internet_data, 'viz/internet_viz.png', 1, 2, 20, 7)

    # fig = plt.figure(figsize=(20, 12))
    # for i in range(9, len(income_data.columns)):
    #     plot_hist(fig.add_subplot(5, 2, i-8), income_data.iloc[:, i], income_data.columns[i])
    # plt.savefig('viz/income_viz.png')
    # plt.show()

    # x = income_data.iloc[:, 13]
    # mean = np.mean(x.to_numpy())
    # std = np.std(x.to_numpy())/(np.sqrt(len(x)))
    # dist = stats.norm(loc = mean, scale = std)
    # x_vals = np.linspace(10.4, 11.4, 250)
    # pdf = dist.pdf(x_vals)

    # means = []
    # for i in range(1000):
    #     sample = np.random.choice(x, size=len(x), replace=True)
    #     means.append(np.mean(sample))


    # fig = plt.figure(figsize=(10, 10))
    # ax = fig.add_subplot(1, 1,1)
    # ax.hist(means, bins = 100)
    # ax.plot(x_vals, pdf)
    # plt.show()



    


