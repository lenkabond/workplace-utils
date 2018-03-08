#!/usr/bin/env python3
#coding: utf-8

import time
import re
from sys import stdout, stderr
from datetime import datetime

import requests
from selenium import webdriver
from selenium.webdriver.support.ui import Select
from bs4 import BeautifulSoup
import configparser
from argparse import ArgumentParser
from requests.packages.urllib3.exceptions import InsecureRequestWarning

requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

def get_config_info():
    config = configparser.ConfigParser()
    config.read('useless_filters.cfg')
    url = config.get('URL', 'stats_url')
    username = config.get('LOGIN DATA', 'username')
    password = config.get('LOGIN DATA', 'password')
    return (url, username, password)


def get_useless_filters(headers, stats_url, username, password):
    '''Returns the list of links to all the filters that have no matches for the last 10 months'''
    stdout.write('Opening the link to filter statistics. It may take up to 20 minutes.\n')
    try:
        r = requests.get(stats_url, headers=headers, auth=(username, password), verify=False)
    except:
        stderr.write("Couldn't open the link. Please check the info in the config file or your internet connection.\n")
        exit(1)
    soup = BeautifulSoup(r.text, "html.parser")
    a_tags = soup.find_all("a", class_="filter-never-worked ")
    filter_links =[ a['href'] for a in a_tags ]
    return filter_links


def disable_filter(driver, link, username, password):
    '''Disables the filter and moves it to Disabled/Год не срабатывали '''
    link = link[8:]
    link = 'https://' + username + ':' + password +'@' + link
    driver.get(link)
    time.sleep(2)
    # disable the filter
    checkbox = driver.find_element_by_name("filter[disable]")
    checkbox.click()
    time.sleep(1)
    # save changes
    save_button = driver.find_element_by_id("save_button")
    save_button.click()
    # move the filter
    choose_filter_group = Select(driver.find_element_by_id("new_tree_id"))
    choose_filter_group.select_by_value("1368")
    time.sleep(1)
    # save the changes to the filter's location
    save_path = driver.find_element_by_xpath("//a[@onclick=\"FilterChangeTree('" + link.split('/')[-1] +"');\"]")
    save_path.click()

def get_years(soup):
    ''' Get the years when the filter was created and/or updated. For the oldest filters there's no info '''
    creation_table = soup.find("table")
    dates = [ d.lstrip('(') for d in re.findall('\([^)]+(?=\))',creation_table.text)]
    years = [ int(d.split(' ')[2]) for d in dates]
    return years

def is_old(headers, link, username, password, old_year):
    '''Checks if the filter is old or relatively new '''
    filter_number = int(link.split('/')[-1])
    r = requests.get(link, headers=headers, auth=(username, password), verify=False)
    soup = BeautifulSoup(r.text, "html.parser")
    # parse the table and get the dates when the filter was added and/or changed
    years = get_years(soup)
    # consider filters that don't save stats or were created automatically as new
    textareas = soup.find_all("textarea")
    # if the filter doesn't save stats or is automatically created, treat it as new
    for text in textareas:       
        if text.string == "action_set_save_stats(0)" or text.string == "Automatic created filter":
            return False
    # if there's no data and the filter's number is big, treat it as new
    if years == [] and filter_number > 10000:
        return False
    # if at least one date is the current year or the last year, the filter is new
    for year in years:
        if year > old_year:
            return False
    # otherwise the filter is old
    return True

def disable_old_only(headers, filter_links, username, password, old_year):
    counter = 0
    for filter_link in filter_links:
        if is_old(headers, filter_link, username, password, old_year):
            try:
                disable_filter(driver, filter_link, username, password)
                stdout.write('Old: ' + filter_link + ' Filter was disabled and moved.\n')
                counter += 1
            except:
                with open('filter_disabling_errors.txt', 'w') as f:
                    f.write(filter_link)
        
        else:
            stdout.write('New: ' + filter_link + '\n')
    stdout.write('Total old filters: ' + str(counter) + '\n')


def main():
    try:
        stats_url, username, password = get_config_info()
    except:
        stderr.write("Couldn't parse the config file. Please make sure it's in the right format.\n")
        exit(1)
    headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'}
    filter_links = get_useless_filters(headers, stats_url, username, password)
    try:
        driver = webdriver.Chrome('/usr/local/bin/chromedriver')
    except:
        stderr.write("I need a Chrome webdriver for Selenium. Please find it in the internet and put it here: /usr/local/bin/chromedriver.\n")
        exit(1)
    old_year = datetime.now().year - 2
    disable_old_only(headers, filter_links, username, password, old_year)
    driver.close()

if __name__ == '__main__':
    main()