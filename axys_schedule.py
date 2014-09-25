#-------------------------------------------------------------------------------
# Name:        axys_schedule
# Purpose:     Automate update of Axys files and export to text file
#
# Author:      J. Aguilar
#
# Created:     09/04/2014
# Copyright:   (c) Martin Investment Management 2014
# Notes:       Modify delay for full portfolio list in case it is too short
#-------------------------------------------------------------------------------

from datetime import date, datetime, timedelta
from apscheduler.scheduler import Scheduler
import cleaners, time

QUARTER_START = [1,4,7,10]

MODEL_PORTFOLIOS = [
    'best_ideas',
    'tortue_foreign',
    'tortue_adr',
    'global_eco']

PROGRESS_PORTFOLIOS = [
    'i-pt-ct',      # Public School Teacher's Retirement Fund of Chicago
    'i-p2-cc1',     # Officers Annuity and Benefit Fund of Cook County
    'i-p3-pr',      # Commingleng Progress 21st Century Fund
    'i-p4-ny1',     # New York Teachers
    'i-p1-la1']     # LACERS

FIS_GROUP_PORTFOLIOS = [
    'i-f2-bl1',     # Employees' Retirement System of the City of Baltimore
    'i-f4-la1',     # Los Angeles County Employees Retirement Association
    'i-f5-1f',      # City of Los Angeles Fire & Police Pension Fund
    'i-f10-ct',     # California State Teachers' Retirement System
    'i-f13-ch',     # State of Connecticut Horizon Fund
    'i-f7-nye',     # New York City Employees' Retirement System
    'i-f6-p1',      # City of Philadelphia
    'i-f11-f1',     # UAW Retiree Medical Benefits Trust Ford
    'i-f12-gm',     # UAW Retiree Medical Benefits Trust GM
    'i-f14-ch1']    # UAW Retiree Medical Benefits Trust Chrysler

FIS_FOREIGN_PORTFOLIOS = [
    'f-if1-pm',     # Pension Reserves Investment Management Board
    'f-if2-tc',     # Trinity Health Corporation
    'f-if3-tp']     # Trinity Health Pension Plan

QUARTERLY_FIS_PORTFOLIOS = []

MONTHLY_FIS_PORTFOLIOS = []

APRRAISAL_PORTFOLIOS = []

def main():
    sched = Scheduler()
    sched.start()

    c_date = datetime.today()
    date_ = '2013-11-30'
    delay = timedelta(minutes=2)

    all_portfolios = combine_lists(PROGRESS_PORTFOLIOS,FIS_GROUP_PORTFOLIOS)

    # end of week jobs
    #-----------------
    if c_date.weekday == 4:
        # runs at 6pm Friday evening
        sched_date = datetime(c_date.year, c_date.month, c_date.day, 18, 0, 0)
        sched.add_date_job(
            axys_job,
            sched_date,
            [MODEL_PORTFOLIOS,all_portfolios,date_])

    # monthly jobs
    #-------------
    if c_date.day == 1:
        # runs at 10 am
        sched_date = datetime(c_date.year, c_date.month, c_date.day, 10, 0, 0)
        sched.add_date_job(
            axys_job,
            sched_date,
            [MODEL_PORTFOLIOS,all_portfolios,date_])
        sched_date = sched_date + delay

    # keep script 'running' in order to allow the scheduler to stay open and run
    # the jobs added
    time.sleep(60)
    sched.shutdown()

#-------------------------------------------------------------------------------
# axys_job : run updates and download reports
#-------------------------------------------------------------------------------
def axys_job(in1,in2,in3):
    m_check = datetime.today()

    #--------------------------------------------------------------------------
    # progress reports
    #--------------------------------------------------------------------------

    a = cleaners.Performance(cdate=in3,account_list=in2)

    # month report
    a.set_performance(per_type='trailing',period='month',period_len=1)
    a.download()

    # quarter report
    a.set_performance(per_type='qtd',period='quarter',period_len=1)
    a.download()

    for ii in [1,2,3,5]:
    # loop to generate 1, 2, 3, 5 year reports
        a.set_performance(per_type='last',period='year',period_len=ii)
        a.download()

    #--------------------------------------------------------------------------
    # unique to FIS group reports
    #--------------------------------------------------------------------------

    # only runs quarterly
    if m_check.month in QUARTER_START:
        a.account_list = FIS_GROUP_PORTFOLIOS

        # year report
        a.set_performance(per_type='ytd',period='year',period_len=1)
        a.download()

    #--------------------------------------------------------------------------
    # portfolio updates for AFG
    #--------------------------------------------------------------------------

    for ii in in1:
        try:
            current = str(datetime.now())
            print(current + ': Updating portfolio ' + ii + ' for AFG upload.\n')
            x = cleaners.Afg(portfolio=ii,cdate=in3)
            x.run_and_done()
            del(x)
            current = str(datetime.now())
            print(current + ': The portfolio ' + ii + ' has been updated.\n')
        except ValueError:
            print("A ValueError has occured for " + ii +
                ". See logfile.txt for details.")

#-------------------------------------------------------------------------------
# combine_lists : combine list of portfolios that have report downloads
#-------------------------------------------------------------------------------

def combine_lists(*lst):
    out_list = []
    for item in lst:
        out_list += item
    out_list = list(set(out_list))
    return out_list

if __name__ == '__main__':
    main()
