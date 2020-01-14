'''ITCH data main module.

The functions in the module analyze and plot the ITCH data for a week.

This script requires the following modules:
    * itertools.product
    * multiprocessing
    * itch_data_analysis_responses_second
    * itch_data_plot_responses_second
    * itch_data_tools_responses_second

The module contains the following functions:
    * taq_data_plot_generator - generates all the analysis and plots from the
      ITCH data.
    * main - the main function of the script.

.. moduleauthor:: Juan Camilo Henao Londono <www.github.com/juanhenao21>
'''

# -----------------------------------------------------------------------------
# Modules

from itertools import product as iprod
import multiprocessing as mp

import itch_data_analysis_responses_second
import itch_data_plot_responses_second
import itch_data_tools_responses_second

# -----------------------------------------------------------------------------


def itch_data_plot_generator(tickers, dates):
    """Generates all the analysis and plots from the ITCH data.

    :param tickers: list of the string abbreviation of the stocks to be
     analized (i.e. ['AAPL', 'MSFT']).
    :param year: string of the year to be analized (i.e '2016').
    :param dates: list of strings with the date of the data to be extracted
     (i.e. ['2008-01-02', '2008-01-03]).
    :return: None -- The function saves the data in a file and does not return
     a value.
    """

    # Especific functions
    for ticker in tickers:

        year = dates[0].split('-')[0]

        # Self-response
        itch_data_analysis_responses_second \
            .itch_self_response_week_responses_second_data(ticker, dates)

        # Plot
        itch_data_plot_responses_second \
            .itch_self_response_week_avg_responses_second_plot(ticker, year)

    return None

# -----------------------------------------------------------------------------


def main():
    """The main function of the script.

    The main function extract, analyze and plot the data.

    :return: None.
    """

    # Tickers and days to analyze
    tickers = ['AAPL']
    dates_2008 = ['2008-01-07', '2008-01-08', '2008-01-09', '2008-01-10',
                  '2008-01-11']
    dates_2016 = ['2016-03-07', '2016-03-08', '2016-03-09', '2016-03-10',
                  '2016-03-11']

    # Run analysis
    itch_data_plot_generator(tickers, dates_2008)
    itch_data_plot_generator(tickers, dates_2016)

    print('Ay vamos!!')

    return None

# -----------------------------------------------------------------------------


if __name__ == '__main__':
    main()
