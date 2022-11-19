import re
import os
import praw
import time
import calendar
import psycopg2
import pandas as pd
import datetime as dt
import multiprocessing
import concurrent.futures
# from pmaw import PushshiftAPI
from datetime import datetime, timedelta
from psaw import PushshiftAPI
from sqlalchemy import create_engine
from yahoo_fin.stock_info import get_data

class Config:
    def __init__(self,__schema__, __tbl__, __chunksize__):
        # Was running this locally before using the Heroku instance Alek stood up
#         self.alch_conn_str="postgresql://postgres:cse6242@localhost:5432/sm_stock_predict"
#         engine=create_engine(self.alch_conn_str)
#         self.conn=engine.connect()
#         self.conn = psycopg2.connect(database="sm_stock_predict",user='postgres',password='cse6242',host='127.0.0.1',port='5432')
        
        self.conn = psycopg2.connect(database="d3skeejujfn0f0",
                                     user='qcqgpbdohgijqd',
                                     password='f00c9aed2fb5f6482b98796671ff2382e2516067e4f2cdb91c2a94aa38f67139',
                                     host='ec2-3-220-207-90.compute-1.amazonaws.com',
                                     port='5432')
        
        self.__schema__=__schema__
        self.__tbl__=__tbl__  
        self.chunksize=__chunksize__
    
    def Close(self):
        self.conn.close()

class Archive(Config):
    def __init__(self, __schema__, __tbl__, __chunksize__):
        super().__init__(__schema__, __tbl__, __chunksize__)
        
    def Source(self):
        return pd.read_sql(f"Select * From {self.__schema__}.{self.__tbl__}", self.conn)

    def DataDump(self, df):
        chunk_df=[df[i:i+self.chunksize] for i in range(0, df.shape[0], self.chunksize)]
        #Was initially using the mogrify function but fell back to just building it as one string with the header and dataframe values
        inserts=','.join(["%s," if x==len(df.columns) else "%s" for x in df.columns])

        #Probably overkill on this dataset but I do something similar with work by running IO tasks on SQL Server inserts for large queries
        with concurrent.futures.ThreadPoolExecutor(max_workers=(2 * multiprocessing.cpu_count()+1)) as executor:
            [executor.submit(self.ArchiveSet, (i,j.values.tolist()), inserts, df.columns) for i,j in enumerate(chunk_df)]
            
        print(f"Done processing results for {self.__tbl__}")
    
    #each chunk is split into its own worker/task opposed to waiting for each set to finish
    def ArchiveSet(self, tblChnk, inserts, cols):
        print(f"__executing a new batch for {tblChnk[0]}")
        
        try:
            cursor=self.conn.cursor()

            insertTups=[str(tuple(x)) for x in tblChnk[1]]
            insertVals=','.join(i for i in insertTups)
            columns=', '.join([x for x in cols])
            
            insert_stmt=f"INSERT INTO {self.__schema__}.{self.__tbl__}({columns})  VALUES {insertVals}"

            cursor.execute(insert_stmt)
            self.conn.commit()
            cursor.close()

            print(f"__finished archiving batch {tblChnk[0]}__")

        except Exception as exc:
            print(f"__generated an exception: {exc} for chunk id {tblChnk[0]}__")
            print(insert_stmt)


class RedditPull:
    def __init__(self, tickers, subreddits):
        self.tickers=tickers
        self.subreddits=subreddits
        self.api = PushshiftAPI()        

        user_agent="Scaper 1.0 by /u/cse6242"
        self.reddit = praw.Reddit(
            client_id='suI0pOf9QlGV1wQfP21TTw',
            client_secret='1WA5IVIx-A2d7d-T9UjuulA9tWERiA',
            username = "cse6242",
            password = "dva-6242",
            user_agent = user_agent
        )
        
    def RedApi(self, red, start_year=2021, end_year=2022, limit=10):
        
        api_start=time.perf_counter()
        subreddit=list(red.keys())[0]
        
        try:    
            reddit_board={}
            for year in range(start_year, end_year+1):         
                print(f"Running API Pull for Reddit Board: {list(red.keys())[0]} on Year {year}") 
                
                ts_after = int(dt.datetime(year, 1, 1).timestamp())
                ts_before = int(dt.datetime(year+1, 1, 1).timestamp())
            
                submissions_dict = {"id" : [],"url" : [],"title" : [],"score" : [], "num_comments": [], "created_utc" : [], "selftext" : []}
                submission_comments_dict = {"comment_id" : [],"comment_parent_id" : [],"comment_body" : [],"comment_link_id" : []}

                gen = self.api.search_submissions( after=ts_after, before=ts_before, filter=['id'], subreddit=subreddit, limit=limit )
                
                for i, submission_psaw in enumerate(gen):
                    submission_id = submission_psaw.d_['id']
                    submission_praw = self.reddit.submission(id=submission_id)

                    board={}

                    board['title']=submission_praw.title
                    board['year']=year
                    board['user_id']=submission_praw.id
                    board['author']=submission_praw.author
                    board['created_dt']=datetime.utcfromtimestamp(int(submission_praw.created_utc)).strftime('%Y-%m-%d')
                    board['score']=submission_praw.score
                    board['upvote_ratio']=submission_praw.upvote_ratio
                    board['url']=submission_praw.url

                    #reference https://towardsdatascience.com/how-to-collect-a-reddit-dataset-c369de539114
                    # extend the comment tree all the way
                    submission_praw.comments.replace_more(limit=5)
                    # for each comment in flattened comment tree
                    for comment in submission_praw.comments.list():
                        submission_comments_dict["comment_id"].append(comment.id)
                        submission_comments_dict["comment_parent_id"].append(comment.parent_id)
                        submission_comments_dict["comment_body"].append(comment.body)
                        submission_comments_dict["comment_link_id"].append(comment.link_id)

                    board['comments']=submission_comments_dict

                    reddit_board[f"{year}-{i}"]=board
       
        except Exception as exc:
            print(f"__generated an exception: {exc} for board: {list(red.keys())[0]}__")
            
        api_finish=time.perf_counter()
        print(f'__Overall time to complete reddit board {list(red.keys())[0]} was: {round((api_finish - api_start)/60, 2)}')
        
        return {list(red.keys())[0]: reddit_board}
        
    def CallApi(self, start, end, limit):
        runs={}
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=(2 * multiprocessing.cpu_count()+1)) as executor:
     
            results={k:executor.submit(self.RedApi, {k:v}, start, end, limit) for (k,v) in self.subreddits.items()}
            output=[]
            
            for r in concurrent.futures.as_completed(list(results.values())):
                output.append(r.result())
                
        return output


if __name__ == "__main__":

    overall_start=time.perf_counter()

    #####################################################################START YAHOO FINANCE API - ASHLEY
    #####################################################################################################
    #####################################################################################################

    start = "01/01/2016"
    end = "09/30/2022"

    # Pull the data for the selected stocks
    tickers = ["AMZN", "GME", "TSLA", "AMC", "AAPL",  "META", "MSFT", "NFLX", "JPM", "GOOG", "DIS",  
            "SNAP", "NOK", "BB",  "AAP", "BTC-USD", "PFE", "HD", "KO", "MMM", "PLTR", "V", "PG", 
            "JNJ", "SHOP", "SPY", "GOOGL", "BABA", "WISH", "DB",  "OPEN"]

    stck_data = {}
    for tick in tickers:
        stck_data[tick.lower()] = get_data(tick.lower(), start_date= start, end_date= end)

    # Export Stock Data into csv
    stck_data = pd.concat(stck_data.values(), ignore_index= False)
    stck_data.to_csv('~/data/Stock_Data.csv')

    # Pull the data for the stock indexes
    indices = ["^IXIC", "^DJI", "^GSPC"]

    idx_data = {}
    for ind in indices:
        idx_data[ind.lower()] = get_data(ind.lower(), start_date= start, end_date= end)

    idx_data = pd.concat(idx_data.values(), ignore_index= False)
    idx_data.to_csv('~/data/Index_Data.csv')

    stck_data=stck_data.rename_axis('businessdate').reset_index()
    idx_data=idx_data.rename_axis('businessdate').reset_index()

    stck_data=stck_data[['ticker','businessdate','close','open','high','low','adjclose','volume']]
    idx_data=idx_data[['ticker','businessdate','close','open','high','low','adjclose','volume']]

    stck_data['businessdate'] = stck_data['businessdate'].dt.strftime('%Y-%m-%d')
    idx_data['businessdate'] = idx_data['businessdate'].dt.strftime('%Y-%m-%d')

    ######################################################################################END YAHOO API PULL

    ########################################################################START STCK/IDX DB INSERT - Kevin
    if len(stck_data) > 0:
        arch=Archive("setup", "equity_pricing", 500) #schema, table, dataframe into chunksize
        arch.DataDump(stck_data)
        arch.Source() #testing results of import
        arch.Close()

    else: 
        print("Equity data was not loaded due to failed API pull")

    if len(idx_data) > 0:
        arch=Archive("setup", "index_pricing", 500) #schema, table, dataframe into chunksize
        arch.DataDump(idx_data)
        arch.Source() #testing results of import
        arch.Close()

    else: 
        print("Index data was not loaded due to failed API pull")
    ##########################################################################END STCK/IDX DB INSERT - Kevin

    ########################################################################################################
    ########################################################################################################
    ###################################################################################START REDDIT API PULL

    subreddits = {
        'wallstreetbets': {}, 
        'stockmarket': {}, 
        'investing': {}, 
        'trading': {}, 
        'stocks': {}, 
        'finance': {}, 
        'robinhood': {}
    }

    stocks = ["AMZN","GME","TSLA","AMC","AAPL","META","MSFT","NFLX","JPM","GOOG","DIS","SNAP","NOK","BB","AAP","BTC","PFE","HD","KO","MMM","PLTR","V","PG","JNJ","DJIA","GSPC","SHOP","SPY","GOOGL","BABA","WISH","FB","DB","OPEN"]
    
    red=RedditPull(stocks, subreddits)
    reddit_dailys=[]

    #manually setting this for now
    year=2022
    months=9 #stop at September for 2022

    ##Loop all months in the year 
    for m in range(1, 2): #months+1
    #     print(f"Moving to the next month: {m}
        end_dy = calendar.monthrange(year, m)[1]
        
        #previous month Dec if ==Jan
        if m==1: 
            prv_end_dy = calendar.monthrange(year, 12)[1]
            prv_mo = 12
            
        else:
            prv_end_dy = calendar.monthrange(year, m-1)[1]
            prv_mo = m + 1
        
        #Loop all days in the current month 
        for d in range(1, end_dy+1):
            
            if d==1 and m==1: #go back prior prior month and prior year for 1/1/00
    #             print(f"Checkpoint 1 - {year-1}/{prv_mo}/{prv_end_dy} to {year}/{m}/{d}")
    #             args( ts_after, ts_before, end year, limit )
                reddit_dailys.append( 
                            red.CallApi(int(dt.datetime(year-1, prv_mo, prv_end_dy).timestamp()), int(dt.datetime(year, m, d).timestamp()), 10) 
                        ) 
            
            elif d==1: #go back prior prior month
    #             print(f" Checkpoint 2 - {year}/{prv_mo}/{prv_end_dy} to {year}/{m}/{d}")
    #             args( ts_after, ts_before, end year, limit )
                reddit_dailys.append( 
                            red.CallApi(int(dt.datetime(year, prv_mo, prv_end_dy).timestamp()), int(dt.datetime(year, m, d).timestamp()), 10) 
                        ) 
            
            else:
    #             print(f" Checkpoint 3 - {year}/{m}/{d-1} to {year}/{m}/{d}")
    #             args( ts_after, ts_before, end year, limit )
                reddit_dailys.append( 
                        red.CallApi(int(dt.datetime(year, m, d-1).timestamp()), int(dt.datetime(year, m, d).timestamp()), 10) 
                    ) 
                    
    reddit_dfs=[]

    def RedditDf(ind=int, key=str, rDict=dict):   
        try:
            df=pd.DataFrame(rDict[key])
            df.insert(0, 'subreddit', key, allow_duplicates=True)
            return df
        
        except Exception as exc:
                print(f"__generated an exception: {exc} for board: {key}__")
        
    for apiresults in reddit_dailys:
        for i, d in enumerate(apiresults):
            reddit_dfs.append( RedditDf(i, list(apiresults[i].keys())[0], d) )
        
    reddit_comments=pd.concat(reddit_dfs, axis=0)

    def clean_comments(coms):
        comments=[s for s in coms if not bool(re.search('(This topic has been removed)|(Your submission was automatically removed)|(I am a bot from rwallstreetbets)', s, re.I))]
        return ','.join([re.sub(r'[^A-Za-z0-9 \,\.\!]+', '', s) for s in comments])

    def clean_author(auth):
        author=re.findall(r"(?<=Redditor\(name=\').*(?=\'\))", str(auth))
        return author if len(author) > 0 else ''

    def clean_text(_str):
        return re.sub(r'[^A-Za-z0-9 \,\.\!]+', '', _str)

    reddit_comments.loc[:,'title']=reddit_comments.apply(lambda x: clean_text(x.title), axis=1)
    reddit_comments.loc[:,'selftext']=reddit_comments.apply(lambda x: clean_text(x.selftext), axis=1)
    reddit_comments.loc[:,'author']=reddit_comments.apply(lambda x: clean_author(x.author), axis=1)
    reddit_comments.loc[:,'comments']=reddit_comments.apply(lambda x: clean_comments(x.comments), axis=1)

    arch=Archive("setup", "reddit_commentary", 100) #schema, table, chunksize
    arch.DataDump(reddit_comments)

    overall_finish=time.perf_counter()
    print(f'__Overall time to complete was {round((overall_finish - overall_start)/60, 2)}')