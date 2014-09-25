#-------------------------------------------------------------------------------
# Name:        match_test
# Purpose:
#
# Author:      J.Aguilar
#
# Created:     12/05/2014
# Copyright:   (c) Martin Investment Management 2014
#-------------------------------------------------------------------------------

from datetime import date, datetime
import pandas as pd
import numpy as np
import cleaners, time, os, sys, fnmatch

CUST_PATH =  r'C:\python modules'

COUNTRY_MAPPING = {
    'USD':'us',
    'CHF':'ch',
    'JPY':'jp',
    'GBP':'gb',
    'DKK':'dk',
    'HKD':'hk',
    'EUR':'eu'
    }

STRING_DATA = {
    'SEDOL':str,
    'CUSIP':str,
    'Source Account Number':str,
    'Mellon Security ID':str,
    'ISIN':str}

def main():
    bny = bny_loader()
    ss = ss_loader()

    bnyi = bny.set_index('Cusip')
    ssi = ss.set_index('Cusip')

# best to slice off us equities, foreign equities, cash and equilivents into separate datasets
# then merge axys and custodian data for each dataset

def slice_us_equity(df):
    # returns us equity slice
    return(df[df['Type'].isin(['csus','adus'])])

def slice_foreign_equity(df):
    # return foreign equities, Type starts with 'cs', or Class = 'e', and not 'csus'
    pass

def slice_cash(df):
    pass

def load_tester():
    dfa = pd.io.excel.read_excel(r'C:\python modules\us equity axys.xlsx','Sheet1')
    dfc = pd.io.excel.read_excel(r'C:\python modules\us equity cust.xlsx','Sheet1')
    return (dfa.copy(),dfc.copy())

class AppraisalRecon():
    """
    current skeleton class to load in axys appraisal, and custodial appraisals
    and perform any comparisons, logging any issues to an output file


    will compare asset prices, share ammounts, local currency value, usd currency value
    """

    def __init__(self,portfolio_codes = None,custodians = None,cdate = None,run_last=False):
        self.portfolio_codes = portfolio
        self.cdate = cdate
        self.dfa = self.load_axys()
        self.dfc = self.load_cust()

    def load_axys(self):
        """ load axys data"""

        # need to change axys Appraisal to take portfolio code, potentially a portfolio code list, or combine data frames
        app = cleaners.Appraisal(self.portfolio_code,self.cdate)
        app.download()
        return(app.load_raw())

    def load_cust(self):
        """ abstract method load custodial data"""
        pass


    def format_axys_data(self):
        """ format axys data for comparisons"""
        # need to create cusip column for us data, and sedol column for international
        pass

    def compare_records(self):
        # probably best to create groups to filter, and then send groups to each object

        check_us = CheckUSEquity(self.dfa[self.dfa['Asset Type'].isin(['csus','adus'])],self.dfc[self.dfc['Domestic Equity']])
        check_us.run_recon()

        check_foreign = CheckForeignEquity(self.dfa,self.dfc[self.dfc['Foreign Equity']])
        check_foreign.run_recon()

        check_cash = CheckCash(self.dfa,self.dfc[self.dfc['Cash']])
        check_cash.run_recon()

    def _chk_date():
        # check the run date(cdate) against the report date
        pass

class CheckAppraisal:

    def __init__(self,df_axys,df_cust):
        self.dfa = df_axys
        self.dfc = df_cust

        # initialize data match bool
        self.value_match = False

        # initilize mismatch data
        self.dfm = pd.DataFrame()

    def run_recon(self):
        self.index_data()
        self.merge()
        self.value_check()

    def integrity_check(self):
        """ check to make sure indices are unique, and equal to each other. No assets
            should have repeated rows.  If appraisal report has repeat rows, log all repeat
            rows to critical error log, and then continue appraisal with rows removed.
        """

        if not self.dfa.index.is_unique:
            # filter non-unique indices
            #dfa_n = self.dfa[self.dfa.groupby(level=self.dfa.index.name).size() > 1]

            # add non-unique security mismatch
            dfa_n = self.dfa.loc[self.dfa.groupby(level=0).size() > 1,:]
            self.add_mismatch(dfa_n,'duplicated security id in axys data')
##            keepIdx = self.dfa.index - dfa_n.index
            # remove duplicated securities to contirue report
            self.dfa = self.dfa.loc[self.dfa.index - dfa_n.index,:]

            #self.dfa[self.index_field][self.dfa.duplicated(self.index_field)].values
            ValueError('')
        if not self.dfc.index.is_unique:
            dfc_n = self.dfc[self.dfc.groupby(level=0).size() > 1]
            self.add_mismatch(dfa_n,'duplicated security id in custodial data')

            self.dfc = self.dfc.loc[self.dfc.index - dfc_n.index,:]

        # security id's should be equal
        if not self.dfa.index.equals(self.dfc.index):
            self.find_null()

    def _find_dup_idx(self,df_in,msg):
        """ finds duplicate indices in a dataframe. Extracts duplicate indexed
            data into error data, and removes duplicate indexed data from
            main dataset in order to continue reconcoliation process on rest of data"""

        if not df_in.index.is_unique:
            df_dups = df.loc[df.groupby(level=0).size() > 1,:]
            self.add_mismatch(df_dups,msg)
            df_in = df_in.loc[self.dfc.index - dfc_n.index,:]
        return(df_in)


    def index_data(self):
        """ index datasets according to unique identifier """
        self.dfa.set_index(self.index_field,inplace=True)
        self.dfc.set_index(self.index_field,inplace=True)
        self.integrity_check()

    def data_equal(self):
        """ check dataset equality, eventually can use to skip steps"""
        return self.dfa.equals(self.dfc)

    def merge(self):
        """ combine axys and custodial data"""
        self.df = pd.merge(self.dfa,self.dfc,how='outer',left_index=True,right_index=True,suffixes=('_axys','_cust'))

    def value_check(self):
        pass

    def find_null(self):
        """ check to see if any data is mismatched"""

        # axys security not in custodial data
        tf = self.dfa.index.isin(self.dfc.index)
        if not tf.all():
            df_slice = self.dfa[~tf]
            self.add_mismatch(df_slice,'axys security not in custodial data')
            self.dfa = self.dfa[tf]

        # custodial security not in axys data
        tf = self.dfc.index.isin(self.dfa.index)
        if not tf.all():
            df_slice = self.dfc[~tf]
            self.add_mismatch(df_slice,'custodial security not in axys data')
            self.dfc = self.dfc[tf]

    def mismatch_identifer(self,df_ver,ver_cols):
        """ add column, Mismatch, containing comma separated list of mismatched value fields """

        df_slice = self.df[~df_ver['Value Match']]
        df_ver = df_ver[~df_ver['Value Match']]

        # initilize as empty string, and add unmatched value column names
        df_ver['Mismatch'] = ''
        for val,ver in zip(self.value_columns,ver_cols):
            df_ver['Mismatch'][~df_ver[ver]] = df_ver['Mismatch'] + ',' + val

        return(df_ver)

    def add_mismatch(self,df_err,message):
        df_err['Mismatch Message'] = message
        self.dfm = self.dfm.append(df_err)

    def add_matched(self,df_match):
        """ filtered matched data rows """
        self.df_matched = self.df_matched.append(df_match)

    def account_summary(self):
        # total number of value column errors
        df_err_counts = pd.DataFrame(df_ver.ix[:,ver_cols].sum(axis=0)).T
        df_err_counts.rename(columns=dict(zip(diff_cols,ver_cols)),inplace=True)

class CheckUSEquity(CheckAppraisal):

    value_columns = ['Quantity','Price','Cost Basis','Market Value'] # need to include'Unrealized Gain', fx rate,Price Paid, reporting currency price
    index_field = 'Cusip'

    def __init__(self,df_axys,df_cust):
        CheckAppraisal.__init__(self,df_axys,df_cust)

    def value_check(self):

        # create column names for differences and verification
        axys_cols = [col + '_axys' for col in self.value_columns]
        cust_cols = [col + '_cust' for col in self.value_columns]
        diff_cols = [col + ' Difference' for col in self.value_columns]
        ver_cols = [col + ' Verified' for col in self.value_columns]

##        # calculate differences, and verification
##        # should change to df.loc[:,axys_cols].values\
##        df_diff = pd.DataFrame(df[axys_cols].values-df[cust_cols].values,index=df.index,columns=diff_cols)
##        df_ver = pd.DataFrame(df_diff.values == 0,index=df.index,columns = ver_cols)
##        df_ver['Value Match'] = df_ver.all(axis=1)
##
##        self.value_match = df_ver['Value Match'].all()

        # calculate differences
        df_diff = pd.DataFrame(index=self.df.index)
        for d,a,c in zip(diff_cols,axys_cols,cust_cols):
            df_diff[d] = self.df[a] - self.df[c]

        # verify value columns equality
        df_ver = df_diff == 0
        df_ver.rename(columns=dict(zip(diff_cols,ver_cols)),inplace=True)
        df_ver['Value Match'] = df_ver.all(axis=1)
        self.value_match = df_ver['Value Match'].all()

        # add mismatch value identifier, and place data in mismatch set
##        if not self.value_match:
##            # mismatched value data
##            df_slice = self.df[~df_ver['Value Match']]
##            df_ver = df_ver[~df_ver['Value Match']]
##            df_ver['Mismatch'] = ''
##            for val,ver in zip(self.value_columns,ver_cols):
##                df_ver['Mismatch'][~df_ver[ver]] = df_ver['Mismatch'] + ',' + val


        # add error count column per security
        df_ver['Mismatched Value Count'] = len(ver_cols) - df_ver.ix[:,ver_cols].sum(axis=1)

        # add mismatch identifier
        if not self.value_match:
            df_slice = self.df[~df_ver['Value Match']]
            df_ver = self.mismatch_identifer(df_ver,ver_cols)
            self.add_mismatch(df_slice,df_ver['Mismatch'])

        # add difference and verification columns to df
        self.df = self.df.join([df_diff,df_ver],how='left')

        # create error count column
##        summary = pd.DataFrame(columns=['Match Count'])
##        summary['Match Count'] = df_ver['Mismatch']
##        for ii in range(0,len(df_ver)):
##            counter = df_ver.ix[ii][0:(len(df_ver.ix[ii])-2)].squeeze()
##            counter = counter.to_dict()
##            counter = ct(k[1] for k in counter.items())
##            summary['Match Count'][ii] = dict(counter)

##        df_ver = df_ver.merge(summary,how='inner',left_index=True,right_index=True)
##        self.add_mismatch(df_slice,df_ver['Mismatch'])

##        return df_ver, summary

        # check if any differences in values, if not, include column to indicate
        # that recon matched for row, else add to df_err, indicating what isn't matching and amount

class CheckForeignEquity(CheckUSEquity):

    value_columns = [
        'Quantity','Price',
        'Cost Basis','Market Value','Unrealized Gain',
        'fx Rate','Cost Basis Reported','Market Value Reported',
        'Unrealized Gain Reported'] # need to include'Unrealized Gain', fx rate,Price Paid, reporting currency price
    index_field = 'Sedol'

    def __init__(self,df_axys,df_cust):
        CheckUSEquity.__init__(self,df_axys,df_cust)

class Loader:
    """
        load : loads and normalizes data
        locate_dir : locate file to upload in directory
        normalize_data : normalize raw data to standard format
        _load_raw : load raw data file


    """
    ext = ''
    col_map = {}
    sub_dir = ''

    DATA_TYPES = {
        'Quantity':np.int64,
        'Sedol':str,
        'Cusip':str,
        'Account Number':str,
        'ISIN':str,
        'Mellon Security ID':str
    }

    def __init__(self,cdate=None):

        self.basepath = os.path.join(CUST_PATH,self.sub_dir)

        if cdate is not None:
            self.date_type = 'specific_date'
            self.cdate = cdate
        else:
            self.date_type = 'latest'

        pattern = self.create_pattern()
        self.file_name = self.locate_dir(self.basepath,pattern)
        self.dpath = os.path.join(self.basepath,self.file_name)

            #chk is in date_type.isin(date_types)

            #if date

    def load(self):
        df = self.normalize_data(self._load_raw())
        df = self.filter_type(df)
        return(df)

    def _load_raw(self):
        pass

    def normalize_data(self,df):
        df.rename(columns=self.flip_dict(self.col_map),inplace=True)
##        df = self._set_country(df)
        df = self._norm_equity(df)
        df = self._norm_cash(df)
        df = self._norm_bond(df)
        return(df)

    def _norm_equity(self):
        pass

    def _norm_cash(self):
        pass

    def _norm_bond(self):
        pass

    def _set_country(self,df):
        pass

    @staticmethod
    def locate_dir(dpath,pattern):
        # match pattern and find greatest file date
        filtered = fnmatch.filter(os.listdir(dpath),pattern)

        if filtered:
            return(max(filtered))
        else:
            raise IOError('No File Matched')

    def remap_dict(self,d1,d2):
        #return({d1[k]:v for (k,v) in d2.items() if k in d1})
        return({d1.get(k,k):v for (k,v) in d2.items()})

    def flip_dict(self,d):
        return({v:k for k,v in d.items()})

class BnyLoader(Loader):
    ext = '.csv'

    sub_dir = 'workbench_files'
    REPORT_NAME = 'AUTO Asset Detail Holdings'

    col_map = {
        'Account Number':'Reporting Account Number',
        'Report Date':'As of Date',
        'Quantity':'Shares/Par',
        'Cusip':'CUSIP',
        'Sedol':'SEDOL',
        'Price':'Base Price',
        'Unrealized Gain':'Base Unrealized Gain/Loss'}

    def __init__(self,cdate=None):
        Loader.__init__(self,cdate=cdate)

    def _load_raw(self):
        # NOTE: possibly can move to super class if all custodians allow for csv download
        dtype = self.remap_dict(self.col_map,self.DATA_TYPES)
        dfc = pd.read_csv(self.dpath,sep=',',header=0,dtype=dtype)
        return(dfc)

    def _norm_equity(self,dfc):
        dfc['Currency Code Axys'] = dfc['Local Currency Code'].map(COUNTRY_MAPPING)
        dfc['Security Type'] = ''
        dfc['Filter Label'] = ''
        e_mask = dfc['Asset Category Description'] == 'EQUITY'
        adr_mask = dfc['Security Description2'] == 'ADR'
        dfc['Security Type'][e_mask] = 'cs' + dfc['Currency Code Axys'][e_mask]
        dfc['Security Type'][adr_mask] = 'ad' + dfc['Currency Code Axys'][adr_mask]
        return dfc

    def _norm_cash(self,dfc):
        cmone = dfc['Asset Category Description'] == 'CASH & CASH EQUIVALENTS'
        cmtwo = dfc['Asset Type Description'] == 'CASH & CASH EQUIVALENTS'
        c_mask = cmone and cmtwo
        dfc['Security Type'][c_mask] = 'ca' + dfc['Currency Code Axys'][c_mask]
        return dfc

    def _norm_bond(self,dfc):
        b_mask = dfc['Security Type Description'] == 'BOND'
        dfc['Security Type'][b_mask] = 'cq' + dfc['Currency Code Axys'][b_mask]
        dfc['Cash Symbol'] = dfc['Security Type']
        d_mask = dfc['Security Description1'] == 'DIVIDENDS RECEIVABLE'
        i_mask = dfc['Security Description1'] == 'INTEREST RECEIVABLE'
        dfc['Cash Symbol'][d_mask] = dfc['Security Type'][d_mask] + 'divacc'
        dfc['Cash Symbol'][i_mask] = dfc['Security Type'][i_mask] + 'inacc'
        dfc['Cash Symbol'][b_mask] = dfc['Security Type'][b_mask] + dfc['Mellon Security ID'][b_mask]
        return dfc

    def filter_type(self,dfc):
        dfc['Domestic Equity'] = (dfc['Security Type'].str.startswith('cs')) & (dfc['Currency Code Axys'] == 'us')
        dfc['Foreign Equity'] = (dfc['Security Type'].str.startswith('cs')) & (dfc['Currency Code Axys'] != 'us')
        dfc['Cash'] = dfc['Security Type'].str.startswith('ca')
        dfc['Bond'] = dfc['Security Type Description'] == 'BOND'
        return dfc

    def _set_country(self,df):
        pass

    def create_pattern(self):
        if self.date_type == 'latest':
            pattern = self.REPORT_NAME + '_*' + self.ext
        elif self.by_date:
            date_str = self.cdate.strftime('%Y%m%d')
            pattern = self.REPORT_NAME + '_' + date_str + '_*' + self.ext
        return(pattern)

    @staticmethod
    def latest_file(dpath,pattern):
        """ find the greatest file in directory. Since ordered by yyyy-mm-dd this works"""
        # match pattern and find greatest file date
        filtered = fnmatch.filter(os.listdir(dpath),pattern)

        if filtered:
            return(max(filtered))
        else:
            raise IOError('No File Matched')

#-------------------------------------------------------------------------------
# bny_loader : normalize BNY Mellon report
#-------------------------------------------------------------------------------
    def bny_loader(cdate=None,latest=True,report='Asset Detail'):
        read_file = r'C:\python modules\workbench_files\AUTO Asset Detail Holdings_20140604_130809925.csv'

##        ext = 'csv'
##        dpath = os.path.join(CUST_PATH,'workbench_files')
##
##        if cdate is not None:
##            date_str = cdate.strftime('%Y%m%d')
##            pattern = report + '_' + date_str + '_*.' + ext
##        else:
##            pattern = report + '_*.' + ext
##
##        file_name = BnyLoader.latest_file(dpath,pattern)
##        read_file = os.path.join(dpath,file_name)
        dfc = pd.read_csv(read_file,sep=',',dtype=STRING_DATA)

        # conversion of Asset Type column values
        #dfc['Asset Type'].update(dfc['Asset Type'].map({
        #    'CASH & CASH EQUIVALENTS':'c','EQUITY':'e'}))

        # conversion of Country Code column values
        # dfc['Country Code'].update(dfc['Country Code'].map({'NA':'us'}))

        dfc['Currency Code Axys'] = dfc['Local Currency Code'].map(COUNTRY_MAPPING)

        # conversion of Local Currency Code column values
        # dfc['Local Currency Code'].update(dfc['Local Currency Code'].map(COUNTRY_MAPPING))

        # conversion of relevant column headers
        dfc.rename(columns={
            'Reporting Account Number': 'Account Number',
            'As of Date':'Report Date',
            'Shares/Par': 'Quantity',
            'CUSIP': 'Cusip',
            'SEDOL': 'Sedol',
            'Base Price': 'Price',
            'Base Unrealized Gain/Loss': 'Unrealized Gain',
            'Country Code': 'Country'},inplace=True)

        # creation of Security Type column
        dfc['Security Type'] = ''

        #------ equity transformation
        e_mask = dfc['Asset Category Description'] == 'EQUITY'
        adr_mask = dfc['Security Description2'] == 'ADR'
        dfc['Security Type'][e_mask] = 'cs' + dfc['Currency Code Axys'][e_mask]
        dfc['Security Type'][adr_mask] = 'ad' + dfc['Currency Code Axys'][adr_mask]

        #------ cash transformation
        cm1 = dfc['Asset Category Description'] == 'CASH & CASH EQUIVALENTS'
        cm2 = dfc['Asset Type Description'] == 'CASH & CASH EQUIVALENTS'
        c_mask = cm1 + cm2
        dfc['Security Type'][c_mask] = 'ca' + dfc['Currency Code Axys'][c_mask]

        #------ bond transformation
        b_mask = dfc['Security Type Description'] == 'BOND'
        dfc['Security Type'][b_mask] = 'cq' + dfc['Currency Code Axys'][b_mask]

        # creation of Cash Symbol Column
        dfc['Cash Symbol'] = dfc['Security Type']
        d_mask = dfc['Security Description1'] == 'DIVIDENDS RECEIVABLE'
        i_mask = dfc['Security Description1'] == 'INTEREST RECEIVABLE'
        dfc['Cash Symbol'][d_mask] = dfc['Security Type'][d_mask] + 'divacc'
        dfc['Cash Symbol'][i_mask] = dfc['Security Type'][i_mask] + 'inacc'
        dfc['Cash Symbol'][b_mask] = dfc['Security Type'][b_mask] + dfc['Mellon Security ID'][b_mask]

        return dfc,read_file

#-------------------------------------------------------------------------------
# dir_sort: sort a file directory into a list from newest to oldest
#-------------------------------------------------------------------------------
    def dir_sort(path):
        mtime = lambda f: os.stat(os.path.join(path, f)).st_mtime
        return list(sorted(os.listdir(path),key=mtime))

#-------------------------------------------------------------------------------
# ss_loader : normalize State Street report
#-------------------------------------------------------------------------------
def ss_loader():
    dpath = r'C:\python modules\mss_files\Dashboard_05-05-2014_13-07-31.csv'
    dfc = pd.read_csv(dpath,sep=',',header=6,skiprows=(0-5,7),dtype={'CUSIP Number':str})

    # conversion of Currency Code column values
    dfc['Currency Code'].update(dfc['Currency Code'].map({'USD':'us'}))

    # conversion of relevant column headers
    dfc.rename(columns={
        'Shares/Par Value': 'Quantity',
        'CUSIP Number': 'Cusip',
        'Base Price Amount': 'Price',
        'Asset Type': 'Class',
        'Base Market Value': 'Market Value'},inplace=True)

    # creation of Type column
    dfc['Type'] = dfc['Currency Code']
    for ii in range(1,len(dfc['Currency Code'])):
        dfc['Type'][ii] = 'cs' + dfc['Currency Code'][ii]

    return dfc

if __name__ == '__main__':
    main()
