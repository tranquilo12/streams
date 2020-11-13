import pandas as pd
import numpy as np


def b_bands(close: pd.Series, window_size: int = 10, num_of_std: int = 5):
    """
    For
    :param close: 
    :param window_size: 
    :param num_of_std: 
    :return: 
    """
    rolling_mean = close.rolling(window=window_size).mean()
    rolling_std = close.rolling(window=window_size).std()
    upper_band = rolling_mean + (rolling_std * num_of_std)
    lower_band = rolling_mean - (rolling_std * num_of_std)
    return rolling_mean, upper_band, lower_band


def min_max_scaler(close: pd.Series) -> pd.Series:
    """
    Return the values of close, scaled between [0, 1]
    :param close: a pandas series.
    :return: the scaled series
    """
    y = (close - close.min()) / (close.max() - close.min()) * 100
    return y


def cal_returns(
    price: pd.Series, day_on_day: bool = True, day_to_first: bool = False
) -> pd.Series:
    """

    :param price:
    :param day_on_day:
    :param day_to_first:
    :return:
    """

    ret = None

    if day_on_day:
        ret = np.log(price).diff()

    if day_to_first:
        ret = np.log(price) - np.log(price.iloc[0])

    return ret
