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
from psaw import PushshiftAPI
from datetime import datetime, timedelta
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
    def __init__(self, subreddits):
        self.subreddits=subreddits
        self.api = PushshiftAPI()        

        user_agent="Scaper 1.0 by /u/cse6242"
        self.reddit = praw.Reddit(
            client_id = 'suI0pOf9QlGV1wQfP21TTw',
            client_secret ='1WA5IVIx-A2d7d-T9UjuulA9tWERiA',
            username = "cse6242",
            password = "dva-6242",
            user_agent = user_agent
        )
        
    def RedApi(self, red, ts_after=int, ts_before=int, limit=10):
        api_start=time.perf_counter()
        subreddit=list(red.keys())[0]
        
        try:    
#             print(f"Running API Pull for Reddit Board: {list(red.keys())[0]} for {datetime.utcfromtimestamp(int(ts_before)).strftime('%Y-%m-%d')}") 
#             submissions_dict = {"id" : [],"url" : [],"title" : [],"score" : [], "num_comments": [], "created_utc" : [], "selftext" : []}
#             submission_comments_dict = {"comment_id" : [],"comment_parent_id" : [],"comment_body" : [],"comment_link_id" : []}
            
            gen = self.api.search_submissions(after=ts_after, before=ts_before, filter=['id'], subreddit=subreddit, limit=limit )
                                              
            board={'title':[], 'selftext':[], 'user_id':[], 'author':[], 'created_dt':[], 'score':[], 'upvote_ratio':[], 'url':[], 'comments':[]}

            for i, submission_psaw in enumerate(gen):
                submission_id = submission_psaw.d_['id']
                submission_praw = self.reddit.submission(id=submission_id)

                board['title'].append(submission_praw.title)
                board['selftext'].append(submission_praw.selftext)
                board['user_id'].append(submission_praw.id)
                board['author'].append(submission_praw.author)
                board['created_dt'].append(datetime.utcfromtimestamp(int(submission_praw.created_utc)).strftime('%Y-%m-%d'))
                board['score'].append(submission_praw.score)
                board['upvote_ratio'].append(submission_praw.upvote_ratio)
                board['url'].append(submission_praw.url)
                
                # https://praw.readthedocs.io/en/stable/tutorials/comments.html
                # extend the comment tree all the way
                submission_praw.comments.replace_more(limit=5)
                
                submission_comments_dict = {"comment_body" : []}
                
                # for each comment in flattened comment tree
                for comment in submission_praw.comments.list():
                    submission_comments_dict["comment_body"].append(comment.body)
                            
                board['comments'].append(submission_comments_dict["comment_body"])
    
            api_finish=time.perf_counter()
            print(f"__Overall time to complete reddit board {list(red.keys())[0]} for {datetime.utcfromtimestamp(int(ts_before)).strftime('%Y-%m-%d')} was: {round((api_finish - api_start)/60, 2)}__")

            return {list(red.keys())[0]: board}

        except Exception as exc:
            print(f"__generated an exception: {exc} for board: {list(red.keys())[0]}__")
            
        
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

    red=RedditPull(subreddits)
    reddit_dailys=[]

    #manually setting this for now
    year=2022
    months=9 #stop at September for 2022

    ##Loop all months in the year 
    for m in range(1, months+1):
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

    def reddit_df(ind=int, key=str, rDict=dict):   
        try:
            df=pd.DataFrame(rDict[key])
            df.insert(0, 'subreddit', key, allow_duplicates=True)
            return df
        
        except Exception as exc:
                print(f"__generated an exception: {exc} for board: {key}__")
        
    for apiresults in reddit_dailys:
        for i, d in enumerate(apiresults):
            reddit_dfs.append( reddit_df(i, list(apiresults[i].keys())[0], d) )
        
    reddit_comments=pd.concat(reddit_dfs, axis=0)

    def clean_comments(coms):
        filter_str=[
            'This topic has been removed',
            'Your submission was removed because it is a short post',
            'Your submission was automatically removed',
            'I am a bot from rwallstreetbets',
            'Please note that as a topic focused subreddit',
            'Sorry your posthttpswww',
            'was automatically removed'
        ]
        
        pattern='|'.join([f"({x})" for x in filter_str])
        
        #do not include 
        comments=[s for s in coms if not bool(re.search(pattern, s, re.I))]
        return ','.join([re.sub(r'[^A-Za-z0-9 \,\.\!\&\\\:\;\/\@\#\$\%\?]+', '', s) for s in comments])

    def clean_author(auth):
        author=re.findall(r"(?<=Redditor\(name=\').*(?=\'\))", str(auth))
        return author if len(author) > 0 else ''

    def clean_text(_str):
        return re.sub(r'[^A-Za-z0-9 \,\.\!\&\\\:\;\/\@\#\$\%\?]+', '', _str)

    reddit_comments.loc[:,'title']=reddit_comments.apply(lambda x: clean_text(x.title), axis=1)
    reddit_comments.loc[:,'selftext']=reddit_comments.apply(lambda x: clean_text(x.selftext), axis=1)
    reddit_comments.loc[:,'url']=reddit_comments.apply(lambda x: clean_text(x.url), axis=1)
    reddit_comments.loc[:,'author']=reddit_comments.apply(lambda x: clean_author(x.author), axis=1)
    reddit_comments.loc[:,'comments']=reddit_comments.apply(lambda x: clean_comments(x.comments), axis=1)

    reddit_comments=reddit_comments.reset_index(drop=True)

    ##filter out anything that isnt in our stock list
    stocks = {"AMZN":["AMZN", "AMAZON"],
            "GME":["GME", "GAMESTOP", "GAME STOP"],
            "TSLA":["TSLA", "TESLA"],
            "AMC":["AMC"],
            "AAPL":["AAPL", "APPLE"],
            "MSFT":["MSFT", "MICRO", "MICROSOFT"],
            "NFLX":["NFLX", "NETFLIX"],
            "JPM":["JPM", "MORGAN","JPMorgan"],
            "GOOG":["GOOG", "GOOGLE", "ALPHABET"],
            "GOOGL":["GOOG", "GOOGLE", "ALPHABET"],
            "DIS":["DIS", "DISNEY"],
            "SNAP":["SNAP","SNAPCHAT"],
            "NOK":["NOK", "NOKIA"],
            "BB":["BB", "BLACKBERRY", "BLACK BERRY"],
            "AAP":[" AAP ", "ADVTG", "ADVANTAGE"],
            "BTC":["BTC", "BITCOIN", "BTC-USD"],
            "PFE":["PFE", "PFIZER", "PFZR"],
            "HD":["HD", "HOMEDEPOT", "HOME DEPOT"],
            "KO":["KO", "COKE", "COCACOLA", "COCA COLA", "COCA-COLA"],
            "MMM":["MMM", "3M"],
            "PLTR":["PLTR", "PALANTIR"],
            "V":[" V ", " V,", ",V,", "VISA"],
            "PG":["PG", "PROCTOR"],
            "JNJ":["JNJ", "JOHNSON"],
            "DJIA":["DJIA", "DJI","DowJones","Dow Jones"],
            "GSPC":["GSPC", "S&P", "SNP"],
            "SHOP":["SHOP ","SHOP,"],
            "SPY":["SPY", "SPYDER"],
            "BABA":["BABA", "ALIBABA"],
            "WISH":["WISH", " WISH,",",WISH," "ContextLogic"],
            "META":[" FB", " META ", " META,", ",META", "FACEBOOK"],
            "DB":[" DB ", " DB,","Deutsche"],
    #           "OPEN":["OPEN", "OPENDOOR"]
            }

    def token_comments(comments, pattern):
        com_list=[]

        for com in comments:
            if bool(re.search(pattern, com, re.I)):
                com_list.append(com)
        
        return '. '.join([x for x in com_list]) if len(com_list) > 0 else ''

    def build_df(ticker=str, pattern=str):
        index=[]
        titles=list(reddit_comments['title'].values)
        
        ##split the selftext/comments into sentences returning only relevant tickers for this project
        selftext=list(token_comments(re.split(r' *[\.\?!]', s, re.I), pattern)+'.' for s in list(reddit_comments['selftext'].values))
        comments=[token_comments(re.split(r' *[\.\?!]', c, re.I), pattern)+'.' for c in list(reddit_comments['comments'].values)]
        
        ##if title, selftext or comments lack any of our stocks than will ignore
        ##grabbing its index to filter our reddit df
        for ind, t in enumerate(titles):
            if bool(re.search(pattern, t, re.I)):
                index.append(ind)
                
        for ind, s in enumerate(selftext):
            if bool(re.search(pattern, s, re.I)):
                index.append(ind)
            
        for ind, c in enumerate(comments):
            if bool(re.search(pattern, c, re.I)):
                index.append(ind)
        
        try:
            reddit_comments.loc[:,'selftext']=pd.Series(selftext)
            reddit_comments.loc[:,'comments']=pd.Series(comments) #replacing with reduced commentary to only be a sentence with the stock ticker/name
            df=reddit_comments.filter(items=list(set(index)), axis=0) 
            df.insert(1, 'ticker', ticker)
            return df 

        except Exception as exc:
            print(f"__generated an exception: {exc}. Check that title, selftext and commentary are indexed properly__")
    
    
    reddit_df_list=[]

    for k, v in stocks.items():
        pattern='|'.join([f"(?:{x})" for x in v])
        reddit_df_list.append( build_df(k, pattern) )
        
    insert_df=pd.concat(reddit_df_list, axis=0)
    #duplicates were created if title, selftext, comments contained the same ticker
    insert_df=insert_df.drop_duplicates(subset=['subreddit','title','ticker','selftext','comments'], keep='first') 
    insert_df=insert_df.reset_index(drop=True)
        
    arch=Archive("setup", "reddit_commentary", 250) #schema, table, chunksize
    arch.DataDump(insert_df)

    overall_finish=time.perf_counter()
    print(f'__Overall time to complete was {round((overall_finish - overall_start)/60, 2)}')