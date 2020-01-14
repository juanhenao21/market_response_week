'''TAQ data main module.

The functions in the module analyze and plot the TAQ data for a week.

This script requires the following modules:
    * itertools.product
    * multiprocessing
    * taq_data_analysis_responses_second
    * taq_data_plot_responses_second
    * taq_data_tools_responses_second

The module contains the following functions:
    * taq_data_plot_generator - generates all the analysis and plots from the
      TAQ data.
    * main - the main function of the script.

.. moduleauthor:: Juan Camilo Henao Londono <www.github.com/juanhenao21>
'''

# -----------------------------------------------------------------------------
# Modules

from itertools import product as iprod
import multiprocessing as mp

import taq_data_analysis_responses_second
import taq_data_plot_responses_second
import taq_data_tools_responses_second

# -----------------------------------------------------------------------------


def taq_data_plot_generator(tickers, dates, week):
    """Generates all the analysis and plots from the TAQ data.

    :param tickers: list of the string abbreviation of the stocks to be
     analized (i.e. ['AAPL', 'MSFT']).
    :param dates: list of strings with the date of the data to be extracted
     (i.e. ['2008-01-02', '2008-01-03]).
    :param week: string with the number of the first day of the week
     (i.e. '03').
    :return: None -- The function saves the data in a file and does not return
     a value.
    """

    # Parallel computing
    with mp.Pool(processes=mp.cpu_count()) as pool:

        # Basic functions
        pool.starmap(taq_data_analysis_responses_second
                     .taq_midpoint_second_data,
                     iprod(tickers, dates))
        pool.starmap(taq_data_analysis_responses_second
                     .taq_trade_signs_second_data,
                     iprod(tickers, dates))

    # Especific functions
    for ticker in tickers:

        year = dates[0].split('-')[0]

        # Self-response
        taq_data_analysis_responses_second \
            .taq_self_response_week_responses_second_data(ticker, dates)

        # Plot
        taq_data_plot_responses_second \
            .taq_midpoint_second_plot(ticker, dates)
        taq_data_plot_responses_second \
            .taq_self_response_week_avg_responses_second_plot(ticker, year,
                                                              week)

    return None

# -----------------------------------------------------------------------------


def main():
    """The main function of the script.

    The main function extract, analyze and plot the data.

    :return: None.
    """

    # Tickers and days to analyze
    tickers = ['AAPL']
    dates_2008_a = ['2008-01-07', '2008-01-08', '2008-01-09', '2008-01-10',
                    '2008-01-11']
    dates_2008_b = ['2008-03-03', '2008-03-04', '2008-03-05', '2008-03-06',
                    '2008-03-07']

    # Run analysis
    taq_data_plot_generator(tickers, dates_2008_a, '07')
    taq_data_plot_generator(tickers, dates_2008_b, '03')

    print('Ay vamos!!')

    return None

# -----------------------------------------------------------------------------


if __name__ == '__main__':
    main()
