'''TAQ data main module.

The functions in the module run the complete extraction, analysis and plot of
the TAQ data.

This script requires the following modules:
    * itertools.product
    * multiprocessing
    * pandas
    * taq_data_analysis_responses_second
    * taq_data_plot_responses_second
    * taq_data_tools_responses_second

The module contains the following functions:
    * taq_build_from_scratch - extract data to dayly CSV files.
    * taq_data_plot_generator - generates all the analysis and plots from the
      TAQ data.
    * main - the main function of the script.

.. moduleauthor:: Juan Camilo Henao Londono <www.github.com/juanhenao21>
'''

# -----------------------------------------------------------------------------
# Modules

from itertools import product as iprod
import multiprocessing as mp
import os
import pandas as pd
import pickle
import subprocess

import taq_data_analysis_responses_second
import taq_data_plot_responses_second
import taq_data_tools_responses_second

__tau__ = 1000

# -----------------------------------------------------------------------------


def taq_build_from_scratch(tickers, year):
    """ Extracts data to dayly CSV files.

    The original data must be decompressed. The function runs a script in
    C++ to decompress and then extract and filter the data for every day of a
    year in CSV files.

    :param tickers: list of the string abbreviation of the stocks to be
     analized (i.e. ['AAPL', 'MSFT']).
    :param year: string of the year to be analized (i.e '2016').
    :return: None -- The function saves the data in a file and does not return
     a value.
    """

    tickers_rm = tickers[:]

    # Check if there are extracted files from the list of stocks
    for ticker in tickers:
        if(os.path.isfile(
            f'../../taq_data/csv_year_data_{year}/{ticker}_{year}_NASDAQ'
            + f'_quotes.csv')
           and os.path.isfile(
                f'../../taq_data/csv_year_data_{year}/{ticker}_{year}_NASDAQ'
                + f'_trades.csv')):

            print(f'The ticker {ticker} has already the trades and quotes '
                  + f'csv files')
            tickers_rm.remove(ticker)

    if (len(tickers_rm)):
        # Compile and run the C++ script to decompress
        os.chdir(f'../../taq_data/decompress_original_data_{year}/'
                 + f'armadillo-3.920.3/')
        subprocess.call('rm CMakeCache.txt', shell=True)
        subprocess.call('cmake .', shell=True)
        subprocess.call('make', shell=True)
        os.chdir('../')
        abs_path = os.path.abspath('.')
        os.system(
            'g++ main.cpp -std=c++11 -lboost_date_time -lz '
            + f'-I {abs_path}/armadillo-3.920.3/include -o decompress.out')
        os.system(f'mv decompress.out ../original_year_data_{year}/')
        os.chdir(f'../original_year_data_{year}')

        # Parallel computing
        with mp.Pool(processes=mp.cpu_count()) as pool:
            print('Extracting quotes')
            pool.starmap(taq_data_tools_responses_second.taq_decompress,
                         iprod(tickers_rm, [year], ['quotes']))
        with mp.Pool(processes=mp.cpu_count()) as pool:
            print('Extracting trades')
            pool.starmap(taq_data_tools_responses_second.taq_decompress,
                         iprod(tickers_rm, [year], ['trades']))

        subprocess.call('rm decompress.out', shell=True)
        subprocess.call(f'mkdir ../csv_year_data_{year}/', shell=True)
        subprocess.call(f'mv *.csv ../csv_year_data_{year}/', shell=True)

    else:
        print('All the tickers have trades and quotes csv files')

    # Extract dayly data
    with mp.Pool(processes=mp.cpu_count()) as pool:
        print('Extracting dayly data')
        pool.starmap(taq_data_analysis_responses_second.taq_data_extract,
                     iprod(tickers, ['quotes'], [year]))
        pool.starmap(taq_data_analysis_responses_second.taq_data_extract,
                     iprod(tickers, ['trades'], [year]))

    return None

# -----------------------------------------------------------------------------


def taq_data_plot_generator(tickers, year):
    """Generates all the analysis and plots from the TAQ data.

    :param tickers: list of the string abbreviation of the stocks to be
     analized (i.e. ['AAPL', 'MSFT']).
    :param year: string of the year to be analized (i.e '2016').
    :return: None -- The function saves the data in a file and does not return
     a value.
    """

    date_list = taq_data_tools_responses_second.taq_bussiness_days(year)

    # Parallel computing
    with mp.Pool(processes=mp.cpu_count()) as pool:

        # Basic functions
        pool.starmap(taq_data_analysis_responses_second
                     .taq_midpoint_second_data,
                     iprod(tickers, date_list))
        pool.starmap(taq_data_analysis_responses_second
                     .taq_trade_signs_second_data,
                     iprod(tickers, date_list))

    # Especific functions
    # Self-response and self-correlator
    for ticker in tickers:

        taq_data_analysis_responses_second \
            .taq_self_response_year_responses_second_data(ticker, year)
        taq_data_analysis_responses_second \
            .taq_trade_sign_self_correlator_year_responses_second_data(ticker,
                                                                       year)

    ticker_prod = iprod(tickers, tickers)

    # Cross-response and cross-correlator
    for ticks in ticker_prod:

        taq_data_analysis_responses_second \
            .taq_cross_response_year_responses_second_data(ticks[0], ticks[1],
                                                           year)
        taq_data_analysis_responses_second \
            .taq_trade_sign_cross_correlator_year_responses_second_data(
                ticks[0], ticks[1], year)

    # Parallel computing
    with mp.Pool(processes=mp.cpu_count()) as pool:

        # Plot
        pool.starmap(taq_data_plot_responses_second
                     .taq_self_response_year_avg_responses_second_plot,
                     iprod(tickers, [year]))
        pool.starmap(taq_data_plot_responses_second
                     .taq_cross_response_year_avg_responses_second_plot,
                     iprod(tickers, tickers, [year]))
        pool.starmap(taq_data_plot_responses_second
            .taq_trade_sign_self_correlator_year_avg_responses_second_plot,
            iprod(tickers, [year]))
        pool.starmap(taq_data_plot_responses_second
            .taq_trade_sign_cross_correlator_year_avg_responses_second_plot,
            iprod(tickers, tickers, [year]))

    return None

# -----------------------------------------------------------------------------


def main():
    """The main function of the script.

    The main function extract, analyze and plot the data.

    :return: None.
    """

    # Tickers and days to analyze
    tickers = ['AAPL', 'MSFT'] # , 'GS', 'JPM', 'XOM', 'CVX']
    year = '2008'

    # Basic folders
    taq_data_tools_responses_second.taq_start_folders(year)

    # Run analysis
    taq_build_from_scratch(tickers, year)
    taq_data_plot_generator(tickers, year)

    print('Ay vamos!!')

    return None

# -----------------------------------------------------------------------------


if __name__ == '__main__':
    main()
