'''ITCH data analysis module.

The functions in the module analyze the data from the NASDAQ stock market,
computing the self-response functions with the ITCH data. The function is
computed for a week.

This script requires the following modules:
    * itertools.product
    * multiprocessing
    * numpy
    * pandas
    * itch_data_tools_responses_second

The module contains the following functions:
    * itch_self_response_day_responses_second_data - computes the self response
     of a day.
    * ithc_self_response_year_responses_second_data - computes the self
     response of a year.
    * main - the main function of the script.

.. moduleauthor:: Juan Camilo Henao Londono <www.github.com/juanhenao21>
'''

# ----------------------------------------------------------------------------
# Modules

from itertools import product as iprod
import multiprocessing as mp
import numpy as np
import pandas as pd
import pickle

import itch_data_tools_responses_second

__tau__ = 1000

# ----------------------------------------------------------------------------


def itch_self_response_day_responses_second_data(ticker, date):
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
        _, midpoint = pickle.load(open(
                f'../../itch_data/data_extraction_{year}/itch_midpoint_second'
                + f'_data/itch_midpoint_second_data_{year}{month}{day}'
                + f'_{ticker}.pickle', 'rb'))
        _, trade_sign = pickle.load(open(
                f'../../itch_data/data_extraction_{year}/itch_trade_signs'
                + f'_second_data/itch_trade_signs_second_data'
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


def itch_self_response_week_responses_second_data(ticker, dates):
    """Computes the self-response of a week.

    Using the taq_self_response_day_responses_second_data function computes the
    self-response function for a week.

    :param ticker: string of the abbreviation of stock to be analized
     (i.e. 'AAPL').
    :param dates: list of strings with the date of the data to be extracted
     (i.e. ['2008-01-02', '2008-01-03]).
    :return: tuple -- The function returns a tuple with numpy arrays.
    """

    year = dates[0].split('-')[0]

    function_name = itch_self_response_week_responses_second_data.__name__
    itch_data_tools_responses_second \
        .itch_function_header_print_data(function_name, ticker, ticker, year,
                                         '', '')

    self_values = []
    args_prod = iprod([ticker], dates)

    # Parallel computation of the self-responses. Every result is appended to
    # a list
    with mp.Pool(processes=mp.cpu_count()) as pool:
        self_values.append(pool.starmap(
            itch_self_response_day_responses_second_data, args_prod))

    # To obtain the total self-response, I sum over all the self-response
    # values and all the amount of trades (averaging values)
    self_v_final = np.sum(self_values[0], axis=0)

    self_response_val = self_v_final[0] / self_v_final[1]
    self_response_avg = self_v_final[1]

    # Saving data
    itch_data_tools_responses_second \
        .itch_save_data(function_name, self_response_val, ticker, ticker, year,
                        '', '')

    return (self_response_val, self_response_avg)

# ----------------------------------------------------------------------------


def main():
    """The main function of the script.

    The main function is used to test the functions in the script.

    :return: None.
    """

    pass

# ----------------------------------------------------------------------------


if __name__ == "__main__":
    main()
