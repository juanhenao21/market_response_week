'''TAQ data analysis module.

The functions in the module analyze the data from the NASDAQ stock market,
computing the self-response functions with the TAQ data. The function is
computed for a week.

This script requires the following modules:
    * itertools.product
    * multiprocessing
    * numpy
    * pandas
    * pickle
    * taq_data_tools_responses_second

The module contains the following functions:
    * taq_midpoint_trade_data - computes the midpoint price of every trade.
    * taq_midpoint_second_data - computes the midpoint price of every second.
    * taq_trade_signs_trade_data - computes the trade signs of every trade.
    * taq_trade_signs_second_data - computes the trade signs of every second.
    * taq_self_response_day_responses_second_data - computes the self response
     of a day.
    * taq_self_response_year_responses_second_data - computes the self response
     of a year.
    * main - the main function of the script.

.. moduleauthor:: Juan Camilo Henao Londono <www.github.com/juanhenao21>
'''

# ----------------------------------------------------------------------------
# Modules

from itertools import product as iprod
import multiprocessing as mp
import numpy as np
import os
import pandas as pd
import pickle

import taq_data_tools_responses_second

__tau__ = 1000

# ----------------------------------------------------------------------------


def taq_midpoint_trade_data(ticker, date):
    """Computes the midpoint price of every event.

    Using the dayly TAQ data computes the midpoint price of every trade in a
    day. For further calculations, the function returns the values for the time
    range from 9h40 to 15h50.

    :param ticker: string of the abbreviation of the stock to be analized
     (i.e. 'AAPL').
    :param date: string with the date of the data to be extracted
     (i.e. '2008-01-02').
    :return: tuple -- The function returns a tuple with numpy arrays.
    """

    date_sep = date.split('-')
    year = date_sep[0]
    month = date_sep[1]
    day = date_sep[2]

    try:
        # Load data
        # TAQ data gives directly the quotes data in every second that there is
        # a change in the quotes
        data_quotes_trade = pd.read_hdf(
            f'../../taq_data/hdf5_dayly_data_{year}/taq_{ticker}_quotes_'
            + f'{date}.h5', key='/quotes')

        time_q = data_quotes_trade['Time'].to_numpy()
        bid_q = data_quotes_trade['Bid'].to_numpy()
        ask_q = data_quotes_trade['Ask'].to_numpy()

        # Some files are corrupted, so there are some zero values that does not
        # have sense
        condition = ask_q != 0
        time_q = time_q[condition]
        bid_q = bid_q[condition]
        ask_q = ask_q[condition]

        midpoint = (bid_q + ask_q) / 2

        return (time_q, midpoint)

    except FileNotFoundError as e:
        print('No data')
        print(e)
        print()
        return None

# ----------------------------------------------------------------------------


def taq_midpoint_second_data(ticker, date):
    """Computes the midpoint price of every second.

    Using the taq_midpoint_trade_data function computes the midpoint price of
    every second. To fill the time spaces when nothing happens I replicate the
    last value calculated until a change in the price happens.

    :param ticker: string of the abbreviation of the stock to be analized
     (i.e. 'AAPL').
    :param date: string with the date of the data to be extracted
     (i.e. '2008-01-02').
    :return: numpy array.
    """

    date_sep = date.split('-')

    year = date_sep[0]
    month = date_sep[1]
    day = date_sep[2]

    function_name = taq_midpoint_second_data.__name__
    taq_data_tools_responses_second \
        .taq_function_header_print_data(function_name, ticker, ticker, year,
                                        month, day)

    try:
        # Calculate the values of the midpoint price for all the events
        time_q, midpoint_trade = taq_midpoint_trade_data(ticker, date)

        # 34800 s = 9h40 - 57000 s = 15h50
        # Reproducing the paper time values. In the results the time interval
        # for the midpoint is [34800, 56999]
        full_time = np.array(range(34800, 57000))
        midpoint = 0. * full_time

        # Select the last midpoint price of every second. If there is no
        # midpoint price in a second, takes the value of the previous second
        for t_idx, t_val in enumerate(full_time):

            condition = time_q == t_val
            if (np.sum(condition)):
                midpoint[t_idx] = midpoint_trade[condition][-1]

            else:
                midpoint[t_idx] = midpoint[t_idx - 1]

        # Prevent zero values in dates when the first seconds does not have a
        # midpoint price value
        t_pos = 34800
        while (not np.sum(time_q == t_pos)):
            t_pos -= 1
        m_pos = 0
        condition_2 = time_q == t_pos
        while (not midpoint[m_pos]):
            midpoint[m_pos] = midpoint_trade[condition_2][-1]
            m_pos += 1

        assert not np.sum(midpoint == 0)

        # Saving data
        if (not os.path.isdir(f'../../taq_data/responses_second_data_{year}'
                              + f'_{function_name}/')):

            try:
                os.mkdir(f'../../taq_data/responses_second_data_{year}/'
                         + f'{function_name}/')
                print('Folder to save data created')

            except FileExistsError:
                print('Folder exists. The folder was not created')

        pickle.dump(midpoint / 10000,
                    open(f'../../taq_data/responses_second_data_{year}/'
                         + f'{function_name}/{function_name}_midpoint_'
                         + f'{year}{month}{day}_{ticker}.pickle', 'wb'))
        pickle.dump(full_time,
                    open(f'../../taq_data/responses_second_data_{year}/'
                         + f'{function_name}/{function_name}_time.pickle',
                         'wb'))

        print('Data saved')
        print()

        return (full_time, midpoint)

    except TypeError as e:
        return None

# ----------------------------------------------------------------------------


def taq_trade_signs_trade_data(ticker, date):
    """Computes the trade signs of every trade.

    Using the dayly TAQ data computes the trade signs of every trade in a day.
    The trade signs are computed using Eq. 1 of the
    `paper
    <https://link.springer.com/content/pdf/10.1140/epjb/e2016-60818-y.pdf>`_.
    As the trades signs are not directly given by the TAQ data, they must be
    infered by the trades prices.
    For further calculations, the function returns the values for the time
    range from 9h40 to 15h50.

    :param ticker: string of the abbreviation of the stock to be analized
        (i.e. 'AAPL').
    :param date: string with the date of the data to be extracted
     (i.e. '2008-01-02').
    :return: tuple -- The function returns a tuple with numpy arrays.
    """

    date_sep = date.split('-')
    year = date_sep[0]
    month = date_sep[1]
    day = date_sep[2]

    try:
        # Load data
        data_trades_trade = pd.read_hdf(
            f'../../taq_data/hdf5_dayly_data_{year}/taq_{ticker}_trades_'
            + f'{date}.h5', key='/trades')

        time_t = data_trades_trade['Time'].to_numpy()
        ask_t = data_trades_trade['Ask'].to_numpy()

        # All the trades must have a price different to zero
        assert not np.sum(ask_t == 0)

        # Trades identified using equation (1)
        identified_trades = np.zeros(len(time_t))
        identified_trades[-1] = 1

        # Implementation of equation (1). Sign of the price change between
        # consecutive trades

        for t_idx in range(len(time_t)):

            diff = ask_t[t_idx] - ask_t[t_idx - 1]

            if (diff):
                identified_trades[t_idx] = np.sign(diff)

            else:
                identified_trades[t_idx] = identified_trades[t_idx - 1]

        # All the identified trades must be different to zero
        assert not np.sum(identified_trades == 0)

        return (time_t, ask_t, identified_trades)

    except FileNotFoundError as e:
        print('No data')
        print(e)
        print()
        return None

# ----------------------------------------------------------------------------


def taq_trade_signs_second_data(ticker, date):
    """Computes the trade signs of every second.

    Using the taq_trade_signs_trade_data function computes the trade signs of
    every second.
    The trade signs are computed using Eq. 2 of the
    `paper
    <https://link.springer.com/content/pdf/10.1140/epjb/e2016-60818-y.pdf>`_.
    As the trades signs are not directly given by the TAQ data, they must be
    infered by the trades prices.
    For further calculations, the function returns the values for the time
    range from 9h40 to 15h50.
    To fill the time spaces when nothing happens I added zeros indicating that
    there were neither a buy nor a sell.

    :param ticker: string of the abbreviation of the stock to be analized
     (i.e. 'AAPL').
    :param date: string with the date of the data to be extracted
     (i.e. '2008-01-02').
    :return: tuple -- The function returns a tuple with numpy arrays.
    """

    date_sep = date.split('-')

    year = date_sep[0]
    month = date_sep[1]
    day = date_sep[2]

    function_name = taq_trade_signs_second_data.__name__
    taq_data_tools_responses_second \
        .taq_function_header_print_data(function_name, ticker, ticker, year,
                                        month, day)

    try:
        # Calculate the values of the trade signs for all the events
        (time_t, ask_t,
         identified_trades) = taq_trade_signs_trade_data(ticker, date)

        # Reproducing the paper time values. In her results the time interval
        # for the trade signs is [34801, 57000]
        full_time = np.array(range(34801, 57001))
        trade_signs = 0. * full_time
        price_signs = 0. * full_time

        # Implementation of Eq. 2. Trade sign in each second
        for t_idx, t_val in enumerate(full_time):

            condition = (time_t >= t_val) * (time_t < t_val + 1)
            trades_same_t_exp = identified_trades[condition]
            sign_exp = int(np.sign(np.sum(trades_same_t_exp)))
            trade_signs[t_idx] = sign_exp

            if (np.sum(condition)):
                price_signs[t_idx] = ask_t[condition][-1]

        # Saving data
        taq_data_tools_responses_second \
            .taq_save_data(function_name,
                           (full_time, price_signs, trade_signs), ticker,
                           ticker, year, month, day)

        return (full_time, price_signs, trade_signs)

    except TypeError as e:
        return None

# ----------------------------------------------------------------------------


def taq_self_response_day_responses_second_data(ticker, date):
    """Computes the self-response of a day.

    Using the midpoint price and trade signs of a ticker computes the self-
    response during different time lags (:math:`\tau`) for a day.

    :param ticker: string of the abbreviation of the stock to be analized
     (i.e. 'AAPL').
    :param date: string with the date of the data to be extracted
     (i.e. '2008-01-02').
    :return: tuple -- The function returns a tuple with numpy arrays.
    """

    date_sep = date.split('-')

    year = date_sep[0]
    month = date_sep[1]
    day = date_sep[2]

    try:
        # Load data
        midpoint = pickle.load(open(
                f'../../taq_data/responses_second_data_{year}/taq_midpoint'
                + f'_second_data/taq_midpoint_second_data_midpoint'
                + f'_{year}{month}{day}_{ticker}.pickle', 'rb'))
        _, _, trade_sign = pickle.load(open(
                f'../../taq_data/responses_second_data_{year}/taq_trade_signs'
                + f'_second_data/taq_trade_signs_second_data'
                + f'_{year}{month}{day}_{ticker}.pickle', 'rb'))

        assert len(midpoint) == len(trade_sign)

        # Array of the average of each tau. 10^3 s is used in the paper
        self_response_tau = np.zeros(__tau__)
        num = np.zeros(__tau__)

        # Calculating the midpoint price return and the self response function

        # Depending on the tau value
        for tau_idx in range(__tau__):

            trade_sign_tau = trade_sign[:-tau_idx - 1]
            trade_sign_no_0_len = len(trade_sign_tau[trade_sign_tau != 0])
            num[tau_idx] = trade_sign_no_0_len
            # Obtain the midpoint price return. Displace the numerator tau
            # values to the right and compute the return

            # Midpoint price returns
            log_return_sec = (midpoint[tau_idx + 1:]
                              - midpoint[:-tau_idx - 1]) \
                / midpoint[:-tau_idx - 1]

            # Obtain the self response value
            if (trade_sign_no_0_len != 0):
                product = log_return_sec * trade_sign_tau
                self_response_tau[tau_idx] = np.sum(product)

        return (self_response_tau, num)

    except FileNotFoundError as e:
        print('No data')
        print(e)
        print()
        zeros = np.zeros(__tau__)
        return (zeros, zeros)

# ----------------------------------------------------------------------------


def taq_self_response_week_responses_second_data(ticker, dates):
    """Computes the self-response of a year.

    Using the taq_self_response_day_responses_second_data function computes the
    self-response function for a year.

    :param ticker: string of the abbreviation of stock to be analized
     (i.e. 'AAPL').
    :param dates: list of strings with the date of the data to be extracted
     (i.e. ['2008-01-02', '2008-01-03]).
    :return: tuple -- The function returns a tuple with numpy arrays.
    """

    year = dates[0].split('-')[0]

    function_name = taq_self_response_week_responses_second_data.__name__
    taq_data_tools_responses_second \
        .taq_function_header_print_data(function_name, ticker, ticker, year,
                                        '', '')

    self_values = []
    args_prod = iprod([ticker], dates)

    # Parallel computation of the self-responses. Every result is appended to
    # a list
    with mp.Pool(processes=mp.cpu_count()) as pool:
        self_values.append(pool.starmap(
            taq_self_response_day_responses_second_data, args_prod))

    # To obtain the total self-response, I sum over all the self-response
    # values and all the amount of trades (averaging values)
    self_v_final = np.sum(self_values[0], axis=0)

    self_response_val = self_v_final[0] / self_v_final[1]
    self_response_avg = self_v_final[1]

    # Saving data
    taq_data_tools_responses_second \
        .taq_save_data(f"{function_name}_{dates[0].split('-')[-1]}",
                       self_response_val, ticker, ticker, year, '', '')

    return (self_response_val, self_response_avg)

# ----------------------------------------------------------------------------


def main():
    """The main function of the script.

    The main function is used to test the functions in the script.

    :return: None.
    """

    pass

    return None

# ----------------------------------------------------------------------------


if __name__ == "__main__":
    main()
