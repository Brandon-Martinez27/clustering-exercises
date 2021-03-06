import warnings
warnings.filterwarnings("ignore")

import pandas as pd
import numpy as np
import os
from env import host, user, password

###################### Acquire Mall Data ######################

def get_connection(db, user=user, host=host, password=password):
    '''
    This function uses my info from my env file to
    create a connection url to access the Codeup db.
    '''
    return f'mysql+pymysql://{user}:{password}@{host}/{db}'
    
def new_mall_data():
    '''
    This function reads the mall data from the Codeup db into a df,
    write it to a csv file, and returns the df.
    '''
    sql_query = '''SELECT * FROM customers
                 '''
    df = pd.read_sql(sql_query, get_connection('mall_customers'))
    df.to_csv('mall_customers.csv')
    return df

def get_mall_data(cached=False):
    '''
    This function reads in mall data from Codeup database if cached == False 
    or if cached == True reads in mall df from a csv file, returns df
    '''
    if cached or os.path.isfile('mall_df.csv') == False:
        df = new_mall_data()
    else:
        df = pd.read_csv('mall_df.csv', index_col=0)
    return df

###################### Prepare Mall Data ######################

def get_upper_outliers(s, k):
    '''
    Given a series and a cutoff value, k, returns the upper outliers for the
    series.

    The values returned will be either 0 (if the point is not an outlier), or a
    number that indicates how far away from the upper bound the observation is.
    '''
    q1, q3 = s.quantile([.25, .75])
    iqr = q3 - q1
    upper_bound = q3 + k * iqr
    return s.apply(lambda x: max([x - upper_bound, 0]))

def get_lower_outliers(s, k):
    '''
    Given a series and a cutoff value, k, returns the lower outliers for the
    series.

    The values returned will be either 0 (if the point is not an outlier), or a
    number that indicates how far away from the lower bound the observation is.
    '''
    q1, q3 = s.quantile([.25, .75])
    iqr = q3 - q1
    lower_bound = q1 - k * iqr
    return s.apply(lambda x: max([lower_bound - x, 0]))

def mall_split(df):
    from sklearn.model_selection import train_test_split
    
    train_validate, test = train_test_split(df, test_size=.2, 
                                        random_state=123)
    train, validate = train_test_split(train_validate, test_size=.3, 
                                   random_state=123)
    return train, validate, test

def one_hot_encoder(train, validate, test, cols):
    from sklearn.preprocessing import OneHotEncoder
    ohe = OneHotEncoder(sparse=False, categories='auto')

    # fit and transform train to create an array
    train_matrix = ohe.fit_transform(train[cols])

    # transform validate and test to create arrays
    validate_matrix = ohe.transform(validate[cols])
    test_matrix = ohe.transform(test[cols])
    
    # convert arrays to dataframes of encoded column
    train_ohe = pd.DataFrame(train_matrix, columns=ohe.categories_[0], index=train.index)
    validate_ohe = pd.DataFrame(validate_matrix, columns=ohe.categories_[0], index=validate.index)
    test_ohe = pd.DataFrame(test_matrix, columns=ohe.categories_[0], index=test.index)

    # join to the original datasets
    train = train.join(train_ohe)
    validate = validate.join(validate_ohe)
    test = test.join(test_ohe)
    return train, validate, test

def min_max_scale(X_train, X_validate, X_test):
    # import scaler
    from sklearn.preprocessing import MinMaxScaler
    # Create scaler object
    scaler = MinMaxScaler(copy=True).fit(X_train)
    
    # tranform into scaled data (arrays)
    X_train_scaled = scaler.transform(X_train)
    X_validate_scaled = scaler.transform(X_validate)
    X_test_scaled = scaler.transform(X_test)
    
    # Create dataframes out of the scaled arrays that were generated by the scaler tranform.
    X_train_scaled = pd.DataFrame(X_train_scaled, 
                              columns=X_train.columns.values).\
                            set_index([X_train.index.values])

    X_validate_scaled = pd.DataFrame(X_validate_scaled, 
                                columns=X_validate.columns.values).\
                            set_index([X_validate.index.values])

    X_test_scaled = pd.DataFrame(X_test_scaled, 
                                columns=X_test.columns.values).\
                            set_index([X_test.index.values])
    return X_train_scaled, X_validate_scaled, X_test_scaled

def wrangle_mall():
    # acquire mall data from mySQL
    df = get_mall_data()

    # split the data
    train, validate, test = mall_split(df)

    # one hot encoding (gender)
    train, validate, test = one_hot_encoder(train, validate, test, ['gender'])

    # missing values
    train.dropna()
    validate.dropna()
    test.dropna()

    # scaling
    cols_to_scale = ['age', 'annual_income', 'spending_score', 'Male']

    X_train = train[cols_to_scale]
    X_validate = validate[cols_to_scale]
    X_test = test[cols_to_scale]

    X_train_scaled, X_validate_scaled, X_test_scaled = min_max_scale(X_train, X_validate, X_test)

    return X_train_scaled, X_validate_scaled, X_test_scaled

