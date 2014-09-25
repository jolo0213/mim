#-------------------------------------------------------------------------------
# Name:        webscraper
# Purpose:     Functions allow different forms of webscraping based input lists
#
# Author:      J.Aguilar
#
# Created:     04/06/2014
# Copyright:   (c) Martin Investment Management 2014
#-------------------------------------------------------------------------------

from selenium import webdriver
from selenium.webdriver.common.keys import Keys
import time

def main():
    pass

#-------------------------------------------------------------------------------
# ticker_to_cusip : searches for corresponding cusips based on ticker searches
#-------------------------------------------------------------------------------
def ticker_to_cusip(search):
    """
    Search input must be a list of tickers which are string values.

    Example: ['TICK1','TICK2','TICK3']

    Note: searching for only one ticker must still be a list (ie. ['TICK'])
    """

    combo = {}
    browser = webdriver.Firefox()
    browser.get('http://www.quantumonline.com/search.cfm')
    for ticker in search:
        # locate search field and submit search entry
        browser.find_element_by_name('tickersymbol').send_keys(ticker) # input search entry
        browser.find_element_by_name('1.0.1').click() # submit

        # scrapes the search result and formats the data to only store the
        # searched ticker and matched cusip
        try:
            text = browser.find_element_by_xpath('/html/body/font/table/tbody/tr/td[2]/font[2]/center/b').text
            text = text.replace('     ',' ')
            text = text.split()
            combo[ticker] = text[4]

        # if no result is returned, then match N/A with the searched ticker
        except:
            combo[ticker] = 'N/A'
    return combo

#-------------------------------------------------------------------------------
# table_search : finds data based on company name searches on ADR website
#-------------------------------------------------------------------------------
def table_search(items):
    """
    Searches ADR website and scrapes data based on fields.
    Inputs must be lists of dictionaries in the form of:

        [{'name':'example1','sedol':'number1'},
        {'name':'example2','sedol':'number2'},
        {'name':'example3','sedol':'number3'}]
        and so on for however many names you want to look up.

    Both the name and sedol must be strings and can be expanded to more than
    two entries.

    If sedol scraped does not match the sedol of the input search entry, then
    the result is dropped. If the exchange scraped is 'NYSE' then the loop
    will stop scanning through the pulled cusips.

    If a string of more than one word is searched for the name and no results
    appear then the last word will be dropped and the search will be repeated
    until the search returns no result when there is only one word.
    """

    results = []
    browser = webdriver.Firefox()
    for thing in items:
        # set initial conditions for while loop
        table = {}
        search_term = thing['name']
        term_len = len(search_term.split())

        # repeats as long as there is no result from the search term
        while (table == {}):
            browser.get('https://www.adr.com/DRSearch/CustomDRSearch')
            browser.find_element_by_xpath('/html/body/div[2]/div[1]/div[1]/div[1]/table/tbody/tr/td[3]/div').click()
            browser.find_element_by_xpath("id('findMenu')/ul/li[4]").click() # company name
            browser.find_element_by_id('symbolFind').send_keys(search_term) # input search entry
            browser.find_element_by_id('symbolFindGo').click() # submit
            time.sleep(3) # wait for search to finish and results to load
            cusips = browser.find_elements_by_class_name('lastCol')

            # elemets under lastCol return some unnecessary elements, so this
            # will filter out for actual 9 character cusips only
            # unfortunately 'Sponsored' is one of the 9 character results returned
            textify = []
            for cusip in cusips:
                try:
                    if (len(cusip.text) == 9) & (cusip.text != 'Sponsored'):
                        textify.append(cusip.text)
                except:
                    pass

            # loops through the filtered list of cusips
            for cusip in textify:
                # go to cusip page and scrape data
                cusip_path = 'https://www.adr.com/DRDetails/Overview?cusip=' + cusip
                browser.get(cusip_path)
                entries = {}
                clean = "/html/body/div[2]/div[1]/div[4]/div[1]/table/tbody"
                entries['Exchange'] = browser.find_element_by_xpath(clean + "/tr[1]/td[3]").text
                entries['Cusip'] = browser.find_element_by_xpath(clean + "/tr[2]/td[3]").text
                entries['Sedol'] = browser.find_element_by_xpath(clean + "/tr[3]/td[3]").text
                entries['ISIN'] = browser.find_element_by_xpath(clean + "/tr[4]/td[3]").text
                entries['Underlying Exchange'] = browser.find_element_by_xpath(clean + "/tr[1]/td[5]").text
                entries['Underlying Cusip'] = browser.find_element_by_xpath(clean + "/tr[2]/td[5]").text
                entries['Underlying Sedol'] = browser.find_element_by_xpath(clean + "/tr[3]/td[5]").text
                entries['Underlying ISIN'] = browser.find_element_by_xpath(clean + "/tr[4]/td[5]").text
                entries['Ratio (DR:ORD)'] = browser.find_element_by_xpath(clean + "/tr[2]/td[7]").text

                # check statement for whether or not to keep the scraped data
                # if sedol does not match the underlying sedol, the data is dropped
                if thing['sedol'] == entries['Underlying Sedol']:
                    # if the Exchange is 'NYSE' then that result takes priority
                    # and any other cusips for that search name are no longer checked
                    if entries['Exchange'] == 'NYSE':
                        table[cusip] = entries
                        break
                    else:
                        table[cusip] = entries

            # checks if the length of the search term is one or not
            # if one, and no result, then the while loop will break and go to the
            # next item in the for loop for the initial input items
            if (table == {}) & (term_len == 1):
                break
            # if the term is longer than one, then the last word of the search term
            # is dropped and the search is repeated again with the shorter query
            if (table == {}) & (term_len != 1):
                search_split = search_term.split()
                del search_split[-1]
                search_term = ' '.join(search_split)
                term_len = len(search_term)
            # if there is still no result for a one word search term then a
            # blank dictionary will be returned for the search term

        # append the resulting dictionary to the table that will be saved
        results.append({thing['name']:table})
    return results

if __name__ == '__main__':
    main()
