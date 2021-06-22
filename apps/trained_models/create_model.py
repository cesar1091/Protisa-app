from pycaret.regression import *
import pandas as pd
import numpy as np
from pycaret.regression import load_model, predict_model


def cumsell(df,target):
    df['Month'] = df.index.month
    df['Weekday'] = df.index.weekday
    df['Year'] = df.index.year
    df['CumSumByMonth'] = df.groupby(['Year','Month'])[target].cumsum().shift(1)
    df.drop(['Year','Month'],axis=1,inplace=True)
    return df

def lag_sells(df,target):
    lista = []
    for lag in range(5,31,5):
        df[f'LAG_{lag}'] = df[target].shift(lag)
        lista.append(f'LAG_{lag}')
    var_num = ['LAG_5','LAG_10','LAG_15','LAG_20','LAG_25','LAG_30','CumSumByMonth']
    for k in lista:
        for j in range(1,5):
            df[f'{k}-pct-change-{j}'] = df[k].pct_change(periods=j)
            df[f'{k}-diff-{j}'] = df[k].diff(periods=j)
            var_num.append(f'{k}-pct-change-{j}')
            var_num.append(f'{k}-diff-{j}')
    df.dropna(inplace=True)
    return df, var_num

def model_pycaret(dataset,target,level):
    all_results = []
    final_model = {}

    for i in dataset[f'{level}'].unique():
        df = dataset[dataset[f'{level}'] == i].drop(f'{level}',axis=1).set_index('Fecha')
        df = df.resample('B').sum()
        df = df.interpolate()
        df = np.log(df)
        df = cumsell(df,target)
        df.dropna(inplace=True)
        df, var_num = lag_sells(df,target)
        #initialize setup from pycaret.regression
        s = setup(df, target = target, train_size = 0.7, data_split_shuffle = True,numeric_features = var_num,  categorical_features = ['Weekday'],numeric_imputation="mean",categorical_imputation="constant",silent = True, verbose = False, remove_outliers = True, session_id = 123)
        # compare all models and select best one based on MAE
        xgboost = create_model('xgboost', max_depth = 10)
        tuned = tune_model(xgboost,optimize = 'RMSE')  
        # capture the compare result grid and store best model in list
        p = pull().iloc[0:1]
        p['time_series'] = str(i)
        all_results.append(p)
        # finalize model i.e. fit on entire data including test set
        f = finalize_model(tuned)
        # attach final model to a dictionary
        final_model[i] = f
        # save transformation pipeline and model as pickle file 
        save_model(f, model_name=f'{target}/{level}/' + str(i.replace(' ','').replace('/','_')), verbose=False)
        
def call_model(dataset,target,level):
    all_score_df = []
    for i in dataset[f'{level}'].unique():
        df = dataset[dataset[f'{level}'] == i].drop(f'{level}',axis=1).set_index('Fecha')
        df = df.resample('B').sum()
        df = df.interpolate()
        df = np.log(df)
        df = cumsell(df,target)
        df.dropna(inplace=True)
        df, var_num = lag_sells(df,target)
        score_df = df.drop(target,axis=1).iloc[-100:]
        l = load_model(f'{target}/{level}/' + str(i.replace(' ','').replace('/','_')), verbose=False)
        p = predict_model(l, data=score_df)
        p['time_series'] = i
        all_score_df.append(p)
    concat_df = pd.concat(all_score_df, axis=0)
    concat_df['Label'] = np.exp(concat_df['Label'])
    concat_df.head()
    return concat_df